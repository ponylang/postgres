# Project: ponylang/postgres

Pure Pony PostgreSQL driver. Alpha-level. Version 0.2.2.

## Building and Testing

```
make ssl=3.0.x                        # build and run all tests
make unit-tests ssl=3.0.x             # unit tests only (no postgres needed)
make integration-tests ssl=3.0.x      # integration tests (needs postgres)
make build-examples ssl=3.0.x         # compile examples
make start-pg-containers              # docker postgres:14.5 on ports 5432 (plain) and 5433 (SSL)
make stop-pg-containers               # stop docker containers
```

SSL version is mandatory. Tests run with `--sequential`. Integration tests require running PostgreSQL 14.5 containers with SCRAM-SHA-256 default auth and an MD5-only user (user: postgres, password: postgres, database: postgres; md5user: md5user, password: md5pass) — one plain on port 5432 and one with SSL on port 5433. Environment variables: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_SSL_HOST`, `POSTGRES_SSL_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`, `POSTGRES_MD5_USERNAME`, `POSTGRES_MD5_PASSWORD`.

## Dependencies

- `ponylang/ssl` 2.0.0 (MD5 password hashing, SCRAM-SHA-256 crypto primitives via `ssl/crypto`, SSL/TLS via `ssl/net`)
- `ponylang/lori` 0.8.1 (TCP networking, STARTTLS support)

Managed via `corral`.

## GitHub Labels

- `changelog - added`, `changelog - changed`, `changelog - fixed` — **PR-only labels**. CI uses these to auto-generate CHANGELOG entries on merge. Never apply to issues.
- `bug`, `help wanted`, `good first issue`, `documentation`, etc. — issue classification labels.

## Architecture

### Session State Machine

`Session` actor is the main entry point. Constructor takes `ServerConnectInfo` (auth, host, service, ssl_mode) and `DatabaseConnectInfo` (user, password, database). Implements `lori.TCPConnectionActor` and `lori.ClientLifecycleEventReceiver`. The stored `ServerConnectInfo` is accessible via `server_connect_info()` for use by `_CancelSender`. State transitions via `_SessionState` interface with concrete states:

```
_SessionUnopened  --connect (no SSL)-->  _SessionConnected
_SessionUnopened  --connect (SSL)-->     _SessionSSLNegotiating
_SessionUnopened  --fail-->              _SessionClosed
_SessionSSLNegotiating --'S'+TLS ok-->   _SessionConnected
_SessionSSLNegotiating --'N'/fail-->     _SessionClosed
_SessionConnected --MD5 auth ok-->       _SessionLoggedIn
_SessionConnected --MD5 auth fail-->     _SessionClosed
_SessionConnected --SASL challenge-->    _SessionSCRAMAuthenticating
_SessionSCRAMAuthenticating --auth ok--> _SessionLoggedIn
_SessionSCRAMAuthenticating --auth fail--> _SessionClosed
_SessionLoggedIn  --close-->             _SessionClosed
```

State behavior is composed via a trait hierarchy that mixes in capabilities and defaults:
- `_ConnectableState` / `_NotConnectableState` — can/can't receive connection events
- `_ConnectedState` / `_UnconnectedState` — has/doesn't have a live connection
- `_AuthenticableState` / `_NotAuthenticableState` — can/can't authenticate
- `_AuthenticatedState` / `_NotAuthenticated` — has/hasn't authenticated

`_SessionSSLNegotiating` is a standalone class (not using `_ConnectedState`) because it handles raw bytes — the server's SSL response is not a PostgreSQL protocol message, so `_ResponseParser` is not used. It mixes in `_NotConnectableState`, `_NotAuthenticableState`, and `_NotAuthenticated`.

`_SessionSCRAMAuthenticating` handles the multi-step SCRAM-SHA-256 exchange after `_SessionConnected` receives an AuthSASL challenge. It mixes in `_ConnectedState` (for `on_received`/TCP write) and `_NotAuthenticated`. Fields store the client nonce, client-first-bare, password, and expected server signature across the exchange steps.

This design makes illegal state transitions call `_IllegalState()` (panic) by default via the trait hierarchy, so only valid transitions need explicit implementation.

### Query Execution Flow

1. Client calls `session.execute(query, ResultReceiver)` where query is `SimpleQuery`, `PreparedQuery`, or `NamedPreparedQuery`; or `session.prepare(name, sql, PrepareReceiver)` to create a named statement; or `session.close_statement(name)` to destroy one; or `session.copy_in(sql, CopyInReceiver)` to start a COPY FROM STDIN operation; or `session.copy_out(sql, CopyOutReceiver)` to start a COPY TO STDOUT operation; or `session.stream(query, window_size, StreamingResultReceiver)` to start a streaming query
2. `_SessionLoggedIn` queues operations as `_QueueItem` — a union of `_QueuedQuery` (execute), `_QueuedPrepare` (prepare), `_QueuedCloseStatement` (close_statement), `_QueuedCopyIn` (copy_in), `_QueuedCopyOut` (copy_out), and `_QueuedStreamingQuery` (stream)
3. The `_QueryState` sub-state machine manages operation lifecycle:
   - `_QueryNotReady`: initial state after auth, before the first ReadyForQuery arrives
   - `_QueryReady`: server is idle, `try_run_query` dispatches based on queue item type — `SimpleQuery` transitions to `_SimpleQueryInFlight`, `PreparedQuery` and `NamedPreparedQuery` transition to `_ExtendedQueryInFlight`, `_QueuedPrepare` transitions to `_PrepareInFlight`, `_QueuedCloseStatement` transitions to `_CloseStatementInFlight`, `_QueuedCopyIn` transitions to `_CopyInInFlight`, `_QueuedCopyOut` transitions to `_CopyOutInFlight`, `_QueuedStreamingQuery` transitions to `_StreamingQueryInFlight`
   - `_SimpleQueryInFlight`: owns per-query accumulation data (`_data_rows`, `_row_description`), delivers results on `CommandComplete`
   - `_ExtendedQueryInFlight`: same data accumulation and result delivery as `_SimpleQueryInFlight` (duplicated because Pony traits can't have iso fields). Entered after sending Parse+Bind+Describe(portal)+Execute+Sync (unnamed) or Bind+Describe(portal)+Execute+Sync (named)
   - `_PrepareInFlight`: handles Parse+Describe(statement)+Sync cycle. Notifies `PrepareReceiver` on success/failure via `ReadyForQuery`
   - `_CloseStatementInFlight`: handles Close(statement)+Sync cycle. Fire-and-forget (no callback); errors silently absorbed
   - `_CopyInInFlight`: handles COPY FROM STDIN data transfer. Sends the COPY query via simple query protocol, receives `CopyInResponse`, then uses pull-based flow: calls `pg_copy_ready` on the `CopyInReceiver` to request data. Client calls `send_copy_data` (sends CopyData + pulls again), `finish_copy` (sends CopyDone), or `abort_copy` (sends CopyFail). Server responds with CommandComplete+ReadyForQuery on success, or ErrorResponse+ReadyForQuery on failure
   - `_CopyOutInFlight`: handles COPY TO STDOUT data reception. Sends the COPY query via simple query protocol, receives `CopyOutResponse` (silently consumed), then receives server-pushed `CopyData` messages (each delivered via `pg_copy_data` to the `CopyOutReceiver`), `CopyDone` (silently consumed), and finally `CommandComplete` (stores row count) + `ReadyForQuery` (delivers `pg_copy_complete`). On error, `ErrorResponse` delivers `pg_copy_failed` and the session remains usable
   - `_StreamingQueryInFlight`: handles streaming row delivery. Entered after sending Parse+Bind+Describe(portal)+Execute(max_rows)+Flush (unnamed) or Bind+Describe(portal)+Execute(max_rows)+Flush (named). Uses Flush instead of Sync to keep the portal alive between batches. `PortalSuspended` triggers batch delivery via `pg_stream_batch`. Client calls `fetch_more()` (sends Execute+Flush) or `close_stream()` (sends Sync). `CommandComplete` delivers final batch and sends Sync. `ReadyForQuery` delivers `pg_stream_complete` and dequeues. On error, sends Sync (required because no Sync is pending during streaming) and delivers `pg_stream_failed`
4. Response data arrives: `_RowDescriptionMessage` sets column metadata, `_DataRowMessage` accumulates rows
5. `_CommandCompleteMessage` triggers result delivery to receiver
6. `_ReadyForQueryMessage` dequeues completed operation, transitions to `_QueryReady`

Only one operation is in-flight at a time. The queue serializes execution. `query_queue`, `query_state`, `backend_pid`, and `backend_secret_key` are non-underscore-prefixed fields on `_SessionLoggedIn` because other types in this package need cross-type access (Pony private fields are type-private). On shutdown, `_SessionLoggedIn.on_shutdown` calls `query_state.drain_in_flight()` to let the in-flight state handle its own queue item (skipping notification if `on_error_response` already notified the receiver), then drains remaining queued items with `SessionClosed`. This prevents double-notification when `close()` arrives between ErrorResponse and ReadyForQuery delivery.

**Query cancellation:** `session.cancel()` requests cancellation of the currently executing query by opening a separate TCP connection via `_CancelSender` and sending a `CancelRequest`. The `cancel` method on `_SessionState` follows the same "never illegal" contract as `close` — it is a no-op in all states except `_SessionLoggedIn`, where it fires only when a query is in flight (not in `_QueryReady` or `_QueryNotReady`). Cancellation is best-effort; the server may or may not honor it. If cancelled, the query's `ResultReceiver` receives `pg_query_failed` with an `ErrorResponseMessage` (SQLSTATE 57014).

### Protocol Layer

**Frontend (client → server):**
- `_FrontendMessage` primitive: `startup()`, `password()`, `query()`, `parse()`, `bind()`, `describe_portal()`, `describe_statement()`, `execute_msg()`, `close_statement()`, `sync()`, `flush()`, `ssl_request()`, `cancel_request()`, `terminate()`, `sasl_initial_response()`, `sasl_response()`, `copy_data()`, `copy_done()`, `copy_fail()` — builds raw byte arrays with big-endian wire format

**Backend (server → client):**
- `_ResponseParser` primitive: incremental parser consuming from a `Reader` buffer. Returns one parsed message per call, `None` if incomplete, errors on junk.
- `_ResponseMessageParser` primitive: routes parsed messages to the current session state's callbacks. Processes messages synchronously within a query cycle (looping until `ReadyForQuery` or buffer exhaustion), then yields via `s._process_again()` between cycles. This prevents behaviors like `close()` from interleaving between result delivery and query dequeuing. If a callback triggers shutdown during the loop, `on_shutdown` clears the read buffer, causing the next parse to return `None` and exit the loop.

**Supported message types:** AuthenticationOk, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, CommandComplete, CopyInResponse, CopyOutResponse, CopyData, CopyDone, DataRow, EmptyQueryResponse, ErrorResponse, NoticeResponse, NotificationResponse, ParameterStatus, ReadyForQuery, RowDescription, ParseComplete, BindComplete, NoData, CloseComplete, ParameterDescription, PortalSuspended. BackendKeyData is parsed and stored in `_SessionLoggedIn` (`backend_pid`, `backend_secret_key`) for future query cancellation. NotificationResponse is parsed into `_NotificationResponseMessage` and routed to `_SessionLoggedIn.on_notification()`, which delivers `pg_notification` to `SessionStatusNotify`. NoticeResponse is parsed into `NoticeResponseMessage` (using shared `_ResponseFieldBuilder` / `_parse_response_fields` with ErrorResponse) and routed via `on_notice()` to `SessionStatusNotify.pg_notice()`. Notices are delivered in all connected states (including during authentication) since PostgreSQL can send them at any time. ParameterStatus is parsed into `_ParameterStatusMessage` and routed via `on_parameter_status()` to `SessionStatusNotify.pg_parameter_status()`, which delivers a `ParameterStatus` val. Like notices, parameter status messages are delivered in all connected states. PortalSuspended is parsed into `_PortalSuspendedMessage` and routed to `s.state.on_portal_suspended(s)` for streaming batch delivery. Extended query acknowledgment messages (ParseComplete, BindComplete, NoData, etc.) are parsed but silently consumed — they fall through the `_ResponseMessageParser` match without routing since the state machine tracks query lifecycle through data-carrying messages only.

### Public API Types

- `Query` — union type `(SimpleQuery | PreparedQuery | NamedPreparedQuery)`
- `SimpleQuery` — val class wrapping a query string (simple query protocol)
- `PreparedQuery` — val class wrapping a query string + `Array[(String | None)] val` params (extended query protocol, single statement only)
- `NamedPreparedQuery` — val class wrapping a statement name + `Array[(String | None)] val` params (executes a previously prepared named statement)
- `Result` trait — `ResultSet` (rows), `SimpleResult` (no rows), `RowModifying` (INSERT/UPDATE/DELETE with count)
- `Rows` / `Row` / `Field` — result data. `Field.value` is `FieldDataTypes` union
- `FieldDataTypes` = `(Array[U8] val | Bool | F32 | F64 | I16 | I32 | I64 | None | String)`
- `TransactionStatus` — union type `(TransactionIdle | TransactionInBlock | TransactionFailed)`. Reported via `pg_transaction_status` callback on every `ReadyForQuery`.
- `Notification` — val class wrapping channel name, payload string, and notifying backend's process ID. Delivered via `pg_notification` callback.
- `NoticeResponseMessage` — non-fatal PostgreSQL notice with all standard fields (same structure as `ErrorResponseMessage`). Delivered via `pg_notice` callback.
- `ParameterStatus` — val class wrapping a runtime parameter name and value reported by the server. Delivered via `pg_parameter_status` callback during startup and after SET commands.
- `SessionStatusNotify` interface (tag) — lifecycle callbacks (connected, connection_failed, authenticated, authentication_failed, transaction_status, notification, notice, parameter_status, shutdown)
- `ResultReceiver` interface (tag) — `pg_query_result(Session, Result)`, `pg_query_failed(Session, Query, (ErrorResponseMessage | ClientQueryError))`
- `PrepareReceiver` interface (tag) — `pg_statement_prepared(Session, name)`, `pg_prepare_failed(Session, name, (ErrorResponseMessage | ClientQueryError))`
- `CopyInReceiver` interface (tag) — `pg_copy_ready(Session)`, `pg_copy_complete(Session, count)`, `pg_copy_failed(Session, (ErrorResponseMessage | ClientQueryError))`. Pull-based: session calls `pg_copy_ready` after `copy_in` and after each `send_copy_data`, letting the client control data flow
- `CopyOutReceiver` interface (tag) — `pg_copy_data(Session, Array[U8] val)`, `pg_copy_complete(Session, count)`, `pg_copy_failed(Session, (ErrorResponseMessage | ClientQueryError))`. Push-based: server drives the flow, delivering data chunks via `pg_copy_data` and signaling completion via `pg_copy_complete`
- `StreamingResultReceiver` interface (tag) — `pg_stream_batch(Session, Rows)`, `pg_stream_complete(Session)`, `pg_stream_failed(Session, (PreparedQuery | NamedPreparedQuery), (ErrorResponseMessage | ClientQueryError))`. Pull-based: session delivers batches via `pg_stream_batch`; client calls `fetch_more()` for the next batch or `close_stream()` to end early
- `ClientQueryError` trait — `SessionNeverOpened`, `SessionClosed`, `SessionNotAuthenticated`, `DataError`
- `DatabaseConnectInfo` — val class grouping database authentication parameters (user, password, database). Passed to `Session.create()` alongside `ServerConnectInfo`.
- `ServerConnectInfo` — val class grouping connection parameters (auth, host, service, ssl_mode). Passed to `Session.create()` as the first parameter. Also used by `_CancelSender`.
- `SSLMode` — union type `(SSLDisabled | SSLRequired)`. `SSLDisabled` is the default (plaintext). `SSLRequired` wraps an `SSLContext val` for TLS negotiation.
- `ErrorResponseMessage` — full PostgreSQL error with all standard fields
- `AuthenticationFailureReason` = `(InvalidAuthenticationSpecification | InvalidPassword | ServerVerificationFailed | UnsupportedAuthenticationMethod)`

### Type Conversion (PostgreSQL OID → Pony)

In `_RowsBuilder._field_to_type()`:
- 16 (bool) → `Bool` (checks for "t")
- 17 (bytea) → `Array[U8] val` (hex-format decode: strips `\x` prefix, parses hex pairs)
- 20 (int8) → `I64`
- 21 (int2) → `I16`
- 23 (int4) → `I32`
- 700 (float4) → `F32`
- 701 (float8) → `F64`
- Everything else → `String`
- NULL → `None`

### Query Cancellation

`_CancelSender` actor — fire-and-forget actor that sends a `CancelRequest` on a separate TCP connection. PostgreSQL requires cancel requests on a different connection from the one executing the query. No response is expected on the cancel connection — the result (if any) arrives as an `ErrorResponse` on the original session connection. When the session uses `SSLRequired`, the cancel connection performs SSL negotiation before sending the CancelRequest — mirroring the main session's connection setup. If the server refuses SSL or the TLS handshake fails, the cancel is silently abandoned. Created by `_SessionLoggedIn.cancel()` using the session's `ServerConnectInfo`, `backend_pid`, and `backend_secret_key`. Design: [discussion #88](https://github.com/ponylang/postgres/discussions/88).

### Mort Primitives

`_IllegalState` and `_Unreachable` in `_mort.pony`. Print file/line to stderr via FFI and exit. Issue URL: `https://github.com/ponylang/postgres/issues`.

## Test Organization

Tests live in the main `postgres/` package (private test classes), organized across multiple files by concern (`_test_*.pony`). The `Main` test actor in `_test.pony` is the single test registry that lists all tests. Read the individual test files for per-test details.

**Conventions**: `_test.pony` contains shared helpers (`_ConnectionTestConfiguration` for env vars, `_ConnectTestNotify`/`_AuthenticateTestNotify` reused by other files). `_test_response_parser.pony` contains `_Incoming*TestMessage` builder classes that construct raw protocol bytes for mock servers across all test files. `_test_mock_message_reader.pony` contains `_MockMessageReader` for extracting complete PostgreSQL frontend messages from TCP data in mock servers.

**Ports**: Mock server tests use ports in the 7669–7706 range and 9667–9668. **Port 7680 is reserved by Windows** (Update Delivery Optimization) and will fail to bind on WSL2 — do not use it.

## Supported PostgreSQL Features

**SSL/TLS:** Optional SSL negotiation via `SSLRequired`. CVE-2021-23222 mitigated via `expect(1)` before SSLRequest. Design: [discussion #76](https://github.com/ponylang/postgres/discussions/76).

**Authentication:** MD5 password and SCRAM-SHA-256. No SCRAM-SHA-256-PLUS (channel binding), Kerberos, GSS, or certificate auth. Design: [discussion #83](https://github.com/ponylang/postgres/discussions/83).

**Protocol:** Simple query and extended query (parameterized via unnamed and named prepared statements). Parameters are text-format only; type OIDs are inferred by the server. LISTEN/NOTIFY, NoticeResponse, ParameterStatus, COPY FROM STDIN (pull-based), COPY TO STDOUT (push-based), row streaming (portal-based cursors with windowed batch delivery via Execute(max_rows)+Flush+PortalSuspended). No function calls. Full feature roadmap: [discussion #72](https://github.com/ponylang/postgres/discussions/72).

**CI containers:** Stock `postgres:14.5` for plain (port 5432, SCRAM-SHA-256 default) and `ghcr.io/ponylang/postgres-ci-pg-ssl:latest` for SSL (port 5433, SSL + md5user); built via `build-ci-image.yml` workflow dispatch or locally via `.ci-dockerfiles/pg-ssl/build-and-push.bash`. MD5 integration tests connect to the SSL container (without using SSL) because only that container has the md5user.

## PostgreSQL Wire Protocol Reference

For protocol details (message formats, extended query protocol, type OIDs, message type bytes), see the [PostgreSQL protocol documentation](https://www.postgresql.org/docs/14/protocol.html). Key pages: [message formats](https://www.postgresql.org/docs/14/protocol-message-formats.html), [message flow](https://www.postgresql.org/docs/14/protocol-flow.html).
