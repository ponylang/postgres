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
- `ponylang/lori` 0.7.2 (TCP networking, STARTTLS support)

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

1. Client calls `session.execute(query, ResultReceiver)` where query is `SimpleQuery`, `PreparedQuery`, or `NamedPreparedQuery`; or `session.prepare(name, sql, PrepareReceiver)` to create a named statement; or `session.close_statement(name)` to destroy one
2. `_SessionLoggedIn` queues operations as `_QueueItem` — a union of `_QueuedQuery` (execute), `_QueuedPrepare` (prepare), and `_QueuedCloseStatement` (close_statement)
3. The `_QueryState` sub-state machine manages operation lifecycle:
   - `_QueryNotReady`: initial state after auth, waiting for server readiness
   - `_QueryReady`: server is idle, `try_run_query` dispatches based on queue item type — `SimpleQuery` transitions to `_SimpleQueryInFlight`, `PreparedQuery` and `NamedPreparedQuery` transition to `_ExtendedQueryInFlight`, `_QueuedPrepare` transitions to `_PrepareInFlight`, `_QueuedCloseStatement` transitions to `_CloseStatementInFlight`
   - `_SimpleQueryInFlight`: owns per-query accumulation data (`_data_rows`, `_row_description`), delivers results on `CommandComplete`
   - `_ExtendedQueryInFlight`: same data accumulation and result delivery as `_SimpleQueryInFlight` (duplicated because Pony traits can't have iso fields). Entered after sending Parse+Bind+Describe(portal)+Execute+Sync (unnamed) or Bind+Describe(portal)+Execute+Sync (named)
   - `_PrepareInFlight`: handles Parse+Describe(statement)+Sync cycle. Notifies `PrepareReceiver` on success/failure via `ReadyForQuery`
   - `_CloseStatementInFlight`: handles Close(statement)+Sync cycle. Fire-and-forget (no callback); errors silently absorbed
4. Response data arrives: `_RowDescriptionMessage` sets column metadata, `_DataRowMessage` accumulates rows
5. `_CommandCompleteMessage` triggers result delivery to receiver
6. `_ReadyForQueryMessage` dequeues completed operation, transitions to `_QueryReady` (idle) or `_QueryNotReady` (non-idle)

Only one operation is in-flight at a time. The queue serializes execution. `query_queue`, `query_state`, `backend_pid`, and `backend_secret_key` are non-underscore-prefixed fields on `_SessionLoggedIn` because other types in this package need cross-type access (Pony private fields are type-private).

**Query cancellation:** `session.cancel()` requests cancellation of the currently executing query by opening a separate TCP connection via `_CancelSender` and sending a `CancelRequest`. The `cancel` method on `_SessionState` follows the same "never illegal" contract as `close` — it is a no-op in all states except `_SessionLoggedIn`, where it fires only when a query is in flight (not in `_QueryReady` or `_QueryNotReady`). Cancellation is best-effort; the server may or may not honor it. If cancelled, the query's `ResultReceiver` receives `pg_query_failed` with an `ErrorResponseMessage` (SQLSTATE 57014).

### Protocol Layer

**Frontend (client → server):**
- `_FrontendMessage` primitive: `startup()`, `password()`, `query()`, `parse()`, `bind()`, `describe_portal()`, `describe_statement()`, `execute_msg()`, `close_statement()`, `sync()`, `ssl_request()`, `cancel_request()`, `terminate()`, `sasl_initial_response()`, `sasl_response()` — builds raw byte arrays with big-endian wire format

**Backend (server → client):**
- `_ResponseParser` primitive: incremental parser consuming from a `Reader` buffer. Returns one parsed message per call, `None` if incomplete, errors on junk.
- `_ResponseMessageParser` primitive: routes parsed messages to the current session state's callbacks. Processes messages synchronously within a query cycle (looping until `ReadyForQuery` or buffer exhaustion), then yields via `s._process_again()` between cycles. This prevents behaviors like `close()` from interleaving between result delivery and query dequeuing. If a callback triggers shutdown during the loop, `on_shutdown` clears the read buffer, causing the next parse to return `None` and exit the loop.

**Supported message types:** AuthenticationOk, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, ReadyForQuery, RowDescription, ParseComplete, BindComplete, NoData, CloseComplete, ParameterDescription, PortalSuspended. BackendKeyData is parsed and stored in `_SessionLoggedIn` (`backend_pid`, `backend_secret_key`) for future query cancellation. Extended query acknowledgment messages (ParseComplete, BindComplete, NoData, etc.) are parsed but silently consumed — they fall through the `_ResponseMessageParser` match without routing since the state machine tracks query lifecycle through data-carrying messages only.

### Public API Types

- `Query` — union type `(SimpleQuery | PreparedQuery | NamedPreparedQuery)`
- `SimpleQuery` — val class wrapping a query string (simple query protocol)
- `PreparedQuery` — val class wrapping a query string + `Array[(String | None)] val` params (extended query protocol, single statement only)
- `NamedPreparedQuery` — val class wrapping a statement name + `Array[(String | None)] val` params (executes a previously prepared named statement)
- `Result` trait — `ResultSet` (rows), `SimpleResult` (no rows), `RowModifying` (INSERT/UPDATE/DELETE with count)
- `Rows` / `Row` / `Field` — result data. `Field.value` is `FieldDataTypes` union
- `FieldDataTypes` = `(Bool | F32 | F64 | I16 | I32 | I64 | None | String)`
- `SessionStatusNotify` interface (tag) — lifecycle callbacks (connected, connection_failed, authenticated, authentication_failed, shutdown)
- `ResultReceiver` interface (tag) — `pg_query_result(Session, Result)`, `pg_query_failed(Session, Query, (ErrorResponseMessage | ClientQueryError))`
- `PrepareReceiver` interface (tag) — `pg_statement_prepared(Session, name)`, `pg_prepare_failed(Session, name, (ErrorResponseMessage | ClientQueryError))`
- `ClientQueryError` trait — `SessionNeverOpened`, `SessionClosed`, `SessionNotAuthenticated`, `DataError`
- `DatabaseConnectInfo` — val class grouping database authentication parameters (user, password, database). Passed to `Session.create()` alongside `ServerConnectInfo`.
- `ServerConnectInfo` — val class grouping connection parameters (auth, host, service, ssl_mode). Passed to `Session.create()` as the first parameter. Also used by `_CancelSender`.
- `SSLMode` — union type `(SSLDisabled | SSLRequired)`. `SSLDisabled` is the default (plaintext). `SSLRequired` wraps an `SSLContext val` for TLS negotiation.
- `ErrorResponseMessage` — full PostgreSQL error with all standard fields
- `AuthenticationFailureReason` = `(InvalidAuthenticationSpecification | InvalidPassword | ServerVerificationFailed | UnsupportedAuthenticationMethod)`

### Type Conversion (PostgreSQL OID → Pony)

In `_RowsBuilder._field_to_type()`:
- 16 (bool) → `Bool` (checks for "t")
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

Tests live in the main `postgres/` package (private test classes).

**Unit tests** (no external dependencies):
- `_TestFrontendMessage*` — verify wire format of outgoing messages
- `_TestResponseParser*` — verify parsing of individual and sequential backend messages
- `_TestHandlingJunkMessages` — uses a local TCP listener that sends junk; verifies session shuts down
- `_TestUnansweredQueriesFailOnShutdown` — uses a local TCP listener that auto-auths but never responds to queries; verifies queued queries get `SessionClosed` failures
- `_TestPrepareShutdownDrainsPrepareQueue` — uses a local TCP listener that auto-auths but never becomes ready; verifies pending prepare operations get `SessionClosed` failures on shutdown
- `_TestTerminateSentOnClose` — mock server fully authenticates and becomes ready; verifies that closing the session sends a Terminate message ('X') to the server
- `_TestSSLNegotiationRefused` — mock server responds 'N' to SSLRequest; verifies `pg_session_connection_failed` fires
- `_TestSSLNegotiationJunkResponse` — mock server responds with junk byte to SSLRequest; verifies session shuts down
- `_TestSSLNegotiationSuccess` — mock server responds 'S', both sides upgrade to TLS, sends AuthOk+ReadyForQuery; verifies full SSL→auth flow
- `_TestCancelQueryInFlight` — mock server accepts two connections; first authenticates with BackendKeyData(pid, key) and receives query; second receives CancelRequest and verifies 16-byte format with correct magic number, pid, and key
- `_TestSSLCancelQueryInFlight` — same as `_TestCancelQueryInFlight` but with SSL on both connections; verifies that `_CancelSender` performs SSL negotiation before sending CancelRequest
- `_TestScramSha256MessageBuilders` — verifies SCRAM message builder functions produce correct output
- `_TestScramSha256ComputeProof` — verifies SCRAM proof computation against known test vectors
- `_TestSCRAMAuthenticationSuccess` — mock server completes full SCRAM-SHA-256 handshake; verifies `pg_session_authenticated` fires
- `_TestSCRAMUnsupportedMechanism` — mock server offers only unsupported SASL mechanisms; verifies `pg_session_authentication_failed` with `UnsupportedAuthenticationMethod`
- `_TestSCRAMServerVerificationFailed` — mock server sends wrong signature in SASLFinal; verifies `pg_session_authentication_failed` with `ServerVerificationFailed`
- `_TestSCRAMErrorDuringAuth` — mock server sends ErrorResponse 28P01 during SCRAM exchange; verifies `pg_session_authentication_failed` with `InvalidPassword`
- `_TestField*Equality*` / `_TestFieldInequality` — example-based reflexive, structural, symmetric equality and inequality tests for Field
- `_TestRowEquality` / `_TestRowInequality` — example-based equality and inequality tests for Row
- `_TestRowsEquality` / `_TestRowsInequality` — example-based equality and inequality tests for Rows
- `_TestField*Property` — PonyCheck property tests for Field reflexive, structural, and symmetric equality
- `_TestRowReflexiveProperty` / `_TestRowsReflexiveProperty` — PonyCheck property tests for Row/Rows reflexive equality

**Integration tests** (require PostgreSQL, names prefixed `integration/`):
- Connect, ConnectFailure, Authenticate, AuthenticateFailure
- Query/Results, Query/AfterAuthenticationFailure, Query/AfterConnectionFailure, Query/AfterSessionHasBeenClosed
- Query/OfNonExistentTable, Query/CreateAndDropTable, Query/InsertAndDelete, Query/EmptyQuery
- PreparedQuery/Results, PreparedQuery/NullParam, PreparedQuery/OfNonExistentTable
- PreparedQuery/InsertAndDelete, PreparedQuery/MixedWithSimple
- PreparedStatement/Prepare, PreparedStatement/PrepareAndExecute, PreparedStatement/PrepareAndExecuteMultiple
- PreparedStatement/PrepareAndClose, PreparedStatement/PrepareFails, PreparedStatement/PrepareAfterClose
- PreparedStatement/CloseNonexistent, PreparedStatement/PrepareDuplicateName
- PreparedStatement/MixedWithSimpleAndPrepared
- Cancel/Query
- SSL/Connect, SSL/Authenticate, SSL/Query, SSL/Refused, SSL/Cancel
- MD5/Authenticate, MD5/AuthenticateFailure, MD5/QueryResults

Test helpers: `_ConnectionTestConfiguration` reads env vars with defaults. Several test message builder classes (`_Incoming*TestMessage`) construct raw protocol bytes for unit tests.

## Known Issues and TODOs in Code

- `_test_response_parser.pony:6` — TODO: chain-of-messages tests to verify correct buffer advancement across message sequences

## Roadmap

**SSL/TLS negotiation** is implemented. Pass `SSLRequired(sslctx)` to `Session.create()` to enable. Design: [discussion #76](https://github.com/ponylang/postgres/discussions/76). **SCRAM-SHA-256 authentication** is implemented. It is the default PostgreSQL auth method since version 10. Design: [discussion #83](https://github.com/ponylang/postgres/discussions/83). Full feature roadmap: [discussion #72](https://github.com/ponylang/postgres/discussions/72). CI uses stock `postgres:14.5` for the non-SSL container (no md5user, SCRAM-SHA-256 default) and `ghcr.io/ponylang/postgres-ci-pg-ssl:latest` for the SSL container (SSL + md5user init script for backward-compat tests); built via `build-ci-image.yml` workflow dispatch or locally via `.ci-dockerfiles/pg-ssl/build-and-push.bash`. MD5 integration tests connect to the SSL container (without using SSL) because only that container has the md5user.

## Supported PostgreSQL Features

**SSL/TLS:** Optional SSL negotiation via `SSLRequired`. The driver sends an SSLRequest before authentication. If the server accepts ('S'), the connection is upgraded to TLS via lori's `start_tls()`. If refused ('N'), connection fails. CVE-2021-23222 mitigated via `expect(1)` before SSLRequest.

**Authentication:** MD5 password and SCRAM-SHA-256. No SCRAM-SHA-256-PLUS (channel binding), Kerberos, GSS, or certificate auth. Design: [discussion #83](https://github.com/ponylang/postgres/discussions/83).

**Protocol:** Simple query protocol and extended query protocol (parameterized queries via unnamed and named prepared statements). Parameters are text-format only; type OIDs are inferred by the server. No COPY, LISTEN/NOTIFY, or function calls.

## PostgreSQL Wire Protocol Reference

### Message Structure

Every message (except StartupMessage) follows: `Byte1(type) | Int32(length including self but not type byte) | payload`. All integers are big-endian. Strings are null-terminated.

### Extended Query Protocol (Prepared Statements)

Separates query processing into discrete steps with two objects:
- **Prepared Statement**: parsed + analyzed query, not yet executable (no parameter values)
- **Portal**: prepared statement + bound parameter values, ready to execute (like an open cursor)

#### Parse — Create Prepared Statement

**Parse** (`P`): `Byte1('P') Int32(len) String(stmt_name) String(query) Int16(num_param_types) Int32[](param_type_oids)`

- Query must be a single statement. Parameters: `$1`, `$2`, ..., `$n`.
- `stmt_name` = `""` for unnamed. Named statements persist until Close or session end.
- Unnamed statement destroyed by next Parse or simple Query.
- Named statements MUST be Closed before redefinition.
- OID 0 = let server infer type.

**ParseComplete** (`1`): `Byte1('1') Int32(4)`

#### Bind — Create Portal

**Bind** (`B`): `Byte1('B') Int32(len) String(portal_name) String(stmt_name) Int16(num_param_fmt) Int16[](param_fmts) Int16(num_params) [Int32(val_len) Byte[](val)]* Int16(num_result_fmt) Int16[](result_fmts)`

- Format codes: 0=text, 1=binary. Shorthand: 0 entries=all text, 1 entry=applies to all, N entries=one per column.
- Named portals persist until Close or transaction end. Unnamed destroyed by next Bind/Query/txn end.
- ALL portals destroyed at transaction end.
- Query planning typically happens at Bind time.

**BindComplete** (`2`): `Byte1('2') Int32(4)`

#### Describe — Request Metadata

**Describe** (`D`): `Byte1('D') Int32(len) Byte1('S'|'P') String(name)`

- `'S'` (statement): responds with ParameterDescription then RowDescription/NoData.
- `'P'` (portal): responds with RowDescription/NoData only.

**ParameterDescription** (`t`): `Byte1('t') Int32(len) Int16(num_params) Int32[](param_oids)`

**NoData** (`n`): `Byte1('n') Int32(4)` — for statements that return no rows.

#### Execute — Run Portal

**Execute** (`E`): `Byte1('E') Int32(len) String(portal_name) Int32(max_rows)`

- `max_rows` 0 = no limit. Only applies to row-returning queries.
- Responses: `DataRow* + CommandComplete` (done), `DataRow* + PortalSuspended` (more rows available), `EmptyQueryResponse`, or `ErrorResponse`.

**PortalSuspended** (`s`): `Byte1('s') Int32(4)`

#### Close — Destroy Statement/Portal

**Close** (`C`): `Byte1('C') Int32(len) Byte1('S'|'P') String(name)`

- Closing a statement implicitly closes all its portals.
- Not an error to close nonexistent objects.

**CloseComplete** (`3`): `Byte1('3') Int32(4)`

#### Sync — Synchronization Point

**Sync** (`S`): `Byte1('S') Int32(4)`

- Commits (success) or rolls back (error) implicit transactions.
- On error: backend discards messages until Sync, then sends ReadyForQuery.
- Exactly one ReadyForQuery per Sync.

#### Flush — Force Output

**Flush** (`H`): `Byte1('H') Int32(4)`

Forces pending output without ending query cycle or producing ReadyForQuery.

### Extended Query Typical Flow

```
Client: Parse → Bind → Describe(portal) → Execute → Sync
Server: ParseComplete → BindComplete → RowDescription → DataRow* → CommandComplete → ReadyForQuery
```

Equivalence: a simple Query is roughly Parse(unnamed) + Bind(unnamed, no params) + Describe(portal) + Execute(unnamed, 0) + Close(portal) + Sync.

### Named vs Unnamed Lifetime

| Object | Unnamed | Named |
|--------|---------|-------|
| Statement | Until next Parse/Query | Until Close or session end |
| Portal | Until next Bind/Query or txn end | Until Close or txn end |

### Pipelining

Multiple extended query sequences can be sent without waiting. Each Sync is a segment boundary. On error in a segment, backend discards remaining messages until Sync, sends ReadyForQuery, then processes next segment independently.

### Asynchronous Messages

Can arrive between any other messages (must always handle):
- **NoticeResponse** (`N`): informational, same format as ErrorResponse
- **NotificationResponse** (`A`): LISTEN/NOTIFY — `Int32(pid) String(channel) String(payload)`
- **ParameterStatus** (`S`): runtime parameter changes

### Parameter Encoding

- **Text (0)**: default. Human-readable via type's I/O functions. Integers as decimal ASCII, booleans as `"t"`/`"f"`.
- **Binary (1)**: type-specific, big-endian for multi-byte, IEEE 754 for floats. May vary across PG versions for complex types.

### Common Type OIDs

| Type | OID | Type | OID |
|------|-----|------|-----|
| bool | 16 | varchar | 1043 |
| bytea | 17 | date | 1082 |
| int8 | 20 | time | 1083 |
| int2 | 21 | timestamp | 1114 |
| int4 | 23 | timestamptz | 1184 |
| text | 25 | interval | 1186 |
| json | 114 | numeric | 1700 |
| float4 | 700 | uuid | 2950 |
| float8 | 701 | jsonb | 3802 |

### Complete Message Type Bytes

**Frontend**: `Q`=Query, `P`=Parse, `B`=Bind, `D`=Describe, `E`=Execute, `C`=Close, `S`=Sync, `H`=Flush, `X`=Terminate, `p`=PasswordMessage/SASLInitialResponse/SASLResponse

**Backend**: `R`=Auth, `K`=BackendKeyData, `S`=ParameterStatus, `Z`=ReadyForQuery, `T`=RowDescription, `D`=DataRow, `C`=CommandComplete, `I`=EmptyQueryResponse, `1`=ParseComplete, `2`=BindComplete, `3`=CloseComplete, `t`=ParameterDescription, `n`=NoData, `s`=PortalSuspended, `E`=ErrorResponse, `N`=NoticeResponse, `A`=NotificationResponse

## File Layout

```
postgres/                         # Main package (35 files)
  session.pony                    # Session actor + state machine traits + query sub-state machine
  database_connect_info.pony       # DatabaseConnectInfo val class (user, password, database)
  server_connect_info.pony        # ServerConnectInfo val class (auth, host, service, ssl_mode)
  ssl_mode.pony                   # SSLDisabled, SSLRequired, SSLMode types
  simple_query.pony               # SimpleQuery class
  prepared_query.pony             # PreparedQuery class
  named_prepared_query.pony       # NamedPreparedQuery class
  query.pony                      # Query union type
  result.pony                     # Result, ResultSet, SimpleResult, RowModifying
  result_receiver.pony            # ResultReceiver interface
  prepare_receiver.pony           # PrepareReceiver interface
  session_status_notify.pony      # SessionStatusNotify interface
  query_error.pony                # ClientQueryError types
  error_response_message.pony     # ErrorResponseMessage + builder
  field.pony                      # Field class
  field_data_types.pony           # FieldDataTypes union
  row.pony                        # Row class
  rows.pony                       # Rows, RowIterator, _RowsBuilder
  _cancel_sender.pony              # Fire-and-forget cancel request actor
  _frontend_message.pony          # Client-to-server messages
  _backend_messages.pony          # Server-to-client message types
  _message_type.pony              # Protocol message type codes
  _response_parser.pony           # Wire protocol parser
  _response_message_parser.pony   # Routes parsed messages to session state
  _authentication_request_type.pony
  _authentication_failure_reason.pony
  _md5_password.pony              # MD5 password construction
  _scram_sha256.pony              # SCRAM-SHA-256 computation primitive
  _mort.pony                      # Panic primitives
  _test.pony                      # Main test actor + integration tests + SSL/SCRAM negotiation tests
  _test_query.pony                # Query integration tests
  _test_response_parser.pony      # Parser unit tests + test message builders
  _test_frontend_message.pony     # Frontend message unit tests
  _test_equality.pony             # Equality tests for Field/Row/Rows (example + PonyCheck property)
  _test_scram.pony                # SCRAM-SHA-256 computation tests
assets/test-cert.pem              # Self-signed test certificate for SSL unit tests
assets/test-key.pem               # Private key for SSL unit tests
examples/README.md                # Examples overview
examples/query/query-example.pony # Simple query with result inspection
examples/ssl-query/ssl-query-example.pony # SSL-encrypted query with SSLRequired
examples/prepared-query/prepared-query-example.pony # PreparedQuery with params and NULL
examples/named-prepared-query/named-prepared-query-example.pony # Named prepared statements with reuse
examples/crud/crud-example.pony   # Multi-query CRUD workflow
examples/cancel/cancel-example.pony # Query cancellation with pg_sleep
.ci-dockerfiles/pg-ssl/           # Dockerfile + init scripts for SSL-enabled PostgreSQL CI container (SCRAM-SHA-256 default + MD5 user)
```
