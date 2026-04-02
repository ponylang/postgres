# Project: ponylang/postgres

Pure Pony PostgreSQL driver. Alpha-level. Version 0.2.2.

## Building and Testing

```
make ssl=3.0.x                        # build and run all tests
make unit-tests ssl=3.0.x             # unit tests only (no postgres needed)
make test-one t=TestName ssl=3.0.x    # run a single test by name
make integration-tests ssl=3.0.x      # integration tests (needs postgres)
make examples ssl=3.0.x               # compile examples
make start-pg-containers              # docker postgres:14.5 on ports 5432 (plain) and 5433 (SSL)
make stop-pg-containers               # stop docker containers
```

SSL version is mandatory. Tests run with `--sequential`. Integration tests require running PostgreSQL 14.5 containers with SCRAM-SHA-256 default auth and an MD5-only user (user: postgres, password: postgres, database: postgres; md5user: md5user, password: md5pass) — one plain on port 5432 and one with SSL on port 5433. Environment variables: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_SSL_HOST`, `POSTGRES_SSL_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`, `POSTGRES_MD5_USERNAME`, `POSTGRES_MD5_PASSWORD`.

## Dependencies

- `ponylang/ssl` 2.0.1 (MD5 password hashing, SCRAM-SHA-256 crypto primitives via `ssl/crypto`, SSL/TLS via `ssl/net`)
- `ponylang/lori` 0.13.0 (TCP networking, STARTTLS support)

Managed via `corral`.

## GitHub Labels

- `changelog - added`, `changelog - changed`, `changelog - fixed` — **PR-only labels**. CI uses these to auto-generate CHANGELOG entries on merge. Never apply to issues.
- `bug`, `help wanted`, `good first issue`, `documentation`, etc. — issue classification labels.

## Architecture

### Session State Machine

`Session` actor is the main entry point. Constructor takes `ServerConnectInfo` (auth, host, service, ssl_mode), `DatabaseConnectInfo` (user, password, database), and an optional `CodecRegistry` (defaults to `CodecRegistry` with all built-in codecs). Implements `lori.TCPConnectionActor` and `lori.ClientLifecycleEventReceiver`. The stored `ServerConnectInfo` is accessible via `server_connect_info()` for use by `_CancelSender`. State transitions via `_SessionState` interface with concrete states:

```
_SessionUnopened  --connect (no SSL)-->              _SessionConnected
_SessionUnopened  --connect (SSLRequired/Preferred)--> _SessionSSLNegotiating
_SessionUnopened  --fail-->                            _SessionClosed
_SessionSSLNegotiating --'S'+TLS ok-->                 _SessionConnected
_SessionSSLNegotiating --'N' (SSLRequired)-->          _SessionClosed
_SessionSSLNegotiating --'N' (SSLPreferred)-->         _SessionConnected  (plaintext fallback)
_SessionSSLNegotiating --TLS fail-->                   _SessionClosed
_SessionConnected --cleartext auth ok-->                _SessionLoggedIn
_SessionConnected --cleartext auth fail-->             _SessionClosed
_SessionConnected --MD5 auth ok-->                     _SessionLoggedIn
_SessionConnected --MD5 auth fail-->                   _SessionClosed
_SessionConnected --SASL challenge-->                  _SessionSCRAMAuthenticating
_SessionSCRAMAuthenticating --auth ok-->               _SessionLoggedIn
_SessionSCRAMAuthenticating --auth fail-->             _SessionClosed
_SessionLoggedIn  --close-->                           _SessionClosed
```

State behavior is composed via a trait hierarchy that mixes in capabilities and defaults:
- `_ConnectableState` / `_NotConnectableState` — can/can't receive connection events
- `_ConnectedState` / `_UnconnectedState` — has/doesn't have a live connection
- `_AuthenticableState` / `_NotAuthenticableState` — can/can't authenticate
- `_AuthenticatedState` / `_NotAuthenticated` — has/hasn't authenticated

`_SessionSSLNegotiating` is a standalone class (not using `_ConnectedState`) because it handles raw bytes — the server's SSL response is not a PostgreSQL protocol message, so `_ResponseParser` is not used. It mixes in `_NotConnectableState`, `_NotAuthenticableState`, and `_NotAuthenticated`. A `_fallback_on_refusal` field controls behavior when the server responds 'N': `true` for `SSLPreferred` (fall back to plaintext), `false` for `SSLRequired` (fire `pg_session_connection_failed`). TLS handshake failures always fire `pg_session_connection_failed` regardless of this flag.

`_SessionSCRAMAuthenticating` handles the multi-step SCRAM-SHA-256 exchange after `_SessionConnected` receives an AuthSASL challenge. It mixes in `_ConnectedState` (for `on_received`/TCP write) and `_NotAuthenticated`. Fields store the client nonce, client-first-bare, password, and expected server signature across the exchange steps.

This design makes illegal state transitions call `_IllegalState()` (panic) by default via the trait hierarchy, so only valid transitions need explicit implementation. `Session._on_timer()` routes to `state.on_timer()` — no-op in all states except `_SessionLoggedIn`, which verifies the timer token matches `statement_timer` and sends a `_CancelSender` to cancel the timed-out operation.

### Query Execution Flow

1. Client calls `session.execute(query, ResultReceiver)` where query is `SimpleQuery`, `PreparedQuery`, or `NamedPreparedQuery`; or `session.prepare(name, sql, PrepareReceiver)` to create a named statement; or `session.close_statement(name)` to destroy one; or `session.copy_in(sql, CopyInReceiver)` to start a COPY FROM STDIN operation; or `session.copy_out(sql, CopyOutReceiver)` to start a COPY TO STDOUT operation; or `session.stream(query, window_size, StreamingResultReceiver)` to start a streaming query; or `session.pipeline(queries, PipelineReceiver)` to pipeline multiple queries. All except `close_statement` accept an optional `statement_timeout` parameter
2. `_SessionLoggedIn` queues operations as `_QueueItem` — a union of `_QueuedQuery` (execute), `_QueuedPrepare` (prepare), `_QueuedCloseStatement` (close_statement), `_QueuedCopyIn` (copy_in), `_QueuedCopyOut` (copy_out), `_QueuedStreamingQuery` (stream), and `_QueuedPipeline` (pipeline)
3. The `_QueryState` sub-state machine manages operation lifecycle:
   - `_QueryNotReady`: initial state after auth, before the first ReadyForQuery arrives
   - `_QueryReady`: server is idle, `try_run_query` dispatches based on queue item type — `SimpleQuery` transitions to `_SimpleQueryInFlight`, `PreparedQuery` and `NamedPreparedQuery` transition to `_ExtendedQueryInFlight`, `_QueuedPrepare` transitions to `_PrepareInFlight`, `_QueuedCloseStatement` transitions to `_CloseStatementInFlight`, `_QueuedCopyIn` transitions to `_CopyInInFlight`, `_QueuedCopyOut` transitions to `_CopyOutInFlight`, `_QueuedStreamingQuery` transitions to `_StreamingQueryInFlight`, `_QueuedPipeline` transitions to `_PipelineInFlight` (or delivers `pg_pipeline_complete` immediately for empty pipelines)
   - `_SimpleQueryInFlight`: owns per-query accumulation data (`_data_rows`, `_row_description`), delivers results on `CommandComplete`
   - `_ExtendedQueryInFlight`: same data accumulation and result delivery as `_SimpleQueryInFlight` (duplicated because Pony traits can't have iso fields). Entered after sending Parse+Bind+Describe(portal)+Execute+Sync (unnamed) or Bind+Describe(portal)+Execute+Sync (named)
   - `_PrepareInFlight`: handles Parse+Describe(statement)+Sync cycle. Notifies `PrepareReceiver` on success/failure via `ReadyForQuery`
   - `_CloseStatementInFlight`: handles Close(statement)+Sync cycle. Fire-and-forget (no callback); errors silently absorbed
   - `_CopyInInFlight`: handles COPY FROM STDIN data transfer. Sends the COPY query via simple query protocol, receives `CopyInResponse`, then uses pull-based flow: calls `pg_copy_ready` on the `CopyInReceiver` to request data. Client calls `send_copy_data` (sends CopyData + pulls again), `finish_copy` (sends CopyDone), or `abort_copy` (sends CopyFail). Server responds with CommandComplete+ReadyForQuery on success, or ErrorResponse+ReadyForQuery on failure
   - `_CopyOutInFlight`: handles COPY TO STDOUT data reception. Sends the COPY query via simple query protocol, receives `CopyOutResponse` (silently consumed), then receives server-pushed `CopyData` messages (each delivered via `pg_copy_data` to the `CopyOutReceiver`), `CopyDone` (silently consumed), and finally `CommandComplete` (stores row count) + `ReadyForQuery` (delivers `pg_copy_complete`). On error, `ErrorResponse` delivers `pg_copy_failed` and the session remains usable
   - `_StreamingQueryInFlight`: handles streaming row delivery. Entered after sending Parse+Bind+Describe(portal)+Execute(max_rows)+Flush (unnamed) or Bind+Describe(portal)+Execute(max_rows)+Flush (named). Uses Flush instead of Sync to keep the portal alive between batches. `PortalSuspended` triggers batch delivery via `pg_stream_batch`. Client calls `fetch_more()` (sends Execute+Flush) or `close_stream()` (sends Sync). `CommandComplete` delivers final batch and sends Sync. `ReadyForQuery` delivers `pg_stream_complete` and dequeues. On error, sends Sync (required because no Sync is pending during streaming) and delivers `pg_stream_failed`
   - `_PipelineInFlight`: handles pipelined multi-query execution. All N query cycles (Parse+Bind+Describe+Execute+Sync per query) are sent in a single TCP write. Owns per-query accumulation data (`_data_rows`, `_row_description`), tracks `_current_index` into the queries array. `on_command_complete` builds and delivers the Result for the current query. `on_error_response` delivers `pg_pipeline_failed` for the current query. `on_ready_for_query` advances `_current_index` and resets `_error` — transitions to `_QueryReady` only on the final ReadyForQuery. Error isolation: each query has its own Sync boundary, so failures don't affect subsequent queries
4. Response data arrives: `_RowDescriptionMessage` (columns as `Array[(String, U32, U16)] val` — name, OID, format code) sets column metadata, `_DataRowMessage` (columns as `Array[(Array[U8] val | None)] val` — raw bytes) accumulates rows
5. `_CommandCompleteMessage` triggers result delivery to receiver
6. `_ReadyForQueryMessage` dequeues completed operation, transitions to `_QueryReady`

Only one operation is in-flight at a time. The queue serializes execution. `query_queue`, `query_state`, `backend_pid`, `backend_secret_key`, `codec_registry`, and `statement_timer: (lori.TimerToken | None)` are non-underscore-prefixed fields on `_SessionLoggedIn` because other types in this package need cross-type access (Pony private fields are type-private). `codec_registry: CodecRegistry` is received from the `Session` constructor and threaded through the state machine (`_SessionUnopened` → `_ConnectableState.on_connected` → `_SessionSSLNegotiating`/`_SessionConnected` → `_AuthenticableState.on_authentication_ok`/`on_authentication_sasl` → `_SessionSCRAMAuthenticating`/`_SessionLoggedIn`), then passed to all `_RowsBuilder` call sites. On shutdown, `_SessionLoggedIn.on_shutdown` cancels any active statement timer, then calls `query_state.drain_in_flight()` to let the in-flight state handle its own queue item (skipping notification if `on_error_response` already notified the receiver), then drains remaining queued items with `SessionClosed`. This prevents double-notification when `close()` arrives between ErrorResponse and ReadyForQuery delivery.

**Statement timeout:** `execute`, `prepare`, `copy_in`, `copy_out`, `stream`, and `pipeline` accept an optional `statement_timeout: (lori.TimerDuration | None)` parameter. When provided, `_QueryReady.try_run_query()` starts a one-shot timer (via `lori.set_timer()`) after sending wire messages. Each in-flight state's `on_ready_for_query()` cancels the timer (via `lori.cancel_timer()`) before transitioning to `_QueryReady`. For pipelines, the timer is cancelled only on the final `ReadyForQuery`. On expiry, `_SessionLoggedIn.on_timer()` sends a `CancelRequest` via `_CancelSender`, producing the same cancellation flow as `session.cancel()`.

**Query cancellation:** `session.cancel()` requests cancellation of the currently executing query by opening a separate TCP connection via `_CancelSender` and sending a `CancelRequest`. The `cancel` method on `_SessionState` follows the same "never illegal" contract as `close` — it is a no-op in all states except `_SessionLoggedIn`, where it fires only when a query is in flight (not in `_QueryReady` or `_QueryNotReady`). Cancellation is best-effort; the server may or may not honor it. If cancelled, the query's `ResultReceiver` receives `pg_query_failed` with an `ErrorResponseMessage` (SQLSTATE 57014).

### Protocol Layer

**Frontend (client → server):**
- `_FrontendMessage` primitive: `startup()`, `password()`, `query()`, `parse()`, `bind()`, `describe_portal()`, `describe_statement()`, `execute_msg()`, `close_statement()`, `sync()`, `flush()`, `ssl_request()`, `cancel_request()`, `terminate()`, `sasl_initial_response()`, `sasl_response()`, `copy_data()`, `copy_done()`, `copy_fail()` — builds raw byte arrays with big-endian wire format

**Backend (server → client):**
- `_ResponseParser` primitive: incremental parser consuming from a `Reader` buffer. Returns one parsed message per call, `None` if incomplete, errors on junk.
- `_ResponseMessageParser` primitive: routes parsed messages to the current session state's callbacks. Processes messages synchronously within a query cycle (looping until `ReadyForQuery` or buffer exhaustion), then yields via `s._process_again()` between cycles. This prevents behaviors like `close()` from interleaving between result delivery and query dequeuing. If a callback triggers shutdown during the loop, `on_shutdown` clears the read buffer, causing the next parse to return `None` and exit the loop.

**Supported message types:** AuthenticationOk, AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, CommandComplete, CopyInResponse, CopyOutResponse, CopyData, CopyDone, DataRow, EmptyQueryResponse, ErrorResponse, NoticeResponse, NotificationResponse, ParameterStatus, ReadyForQuery, RowDescription, ParseComplete, BindComplete, NoData, CloseComplete, ParameterDescription, PortalSuspended. BackendKeyData is parsed and stored in `_SessionLoggedIn` (`backend_pid`, `backend_secret_key`) for future query cancellation. NotificationResponse is parsed into `_NotificationResponseMessage` and routed to `_SessionLoggedIn.on_notification()`, which delivers `pg_notification` to `SessionStatusNotify`. NoticeResponse is parsed into `NoticeResponseMessage` (using shared `_ResponseFieldBuilder` / `_parse_response_fields` with ErrorResponse) and routed via `on_notice()` to `SessionStatusNotify.pg_notice()`. Notices are delivered in all connected states (including during authentication) since PostgreSQL can send them at any time. ParameterStatus is parsed into `_ParameterStatusMessage` and routed via `on_parameter_status()` to `SessionStatusNotify.pg_parameter_status()`, which delivers a `ParameterStatus` val. Like notices, parameter status messages are delivered in all connected states. PortalSuspended is parsed into `_PortalSuspendedMessage` and routed to `s.state.on_portal_suspended(s)` for streaming batch delivery. Extended query acknowledgment messages (ParseComplete, BindComplete, NoData, etc.) are parsed but silently consumed — they fall through the `_ResponseMessageParser` match without routing since the state machine tracks query lifecycle through data-carrying messages only.

### Public API Types

- `Query` — union type `(SimpleQuery | PreparedQuery | NamedPreparedQuery)`
- `SimpleQuery` — val class wrapping a query string (simple query protocol)
- `PreparedQuery` — val class wrapping a query string + `Array[FieldDataTypes] val` params (extended query protocol, single statement only). Typed params (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`, `PgArray`, `PgTimestamp`, `PgTime`, `PgDate`, `PgInterval`) use binary wire format with explicit OIDs; `String` and `None` use text format with server-inferred types
- `NamedPreparedQuery` — val class wrapping a statement name + `Array[FieldDataTypes] val` params (executes a previously prepared named statement). Same typed parameter semantics as `PreparedQuery`
- `Result` — union type `(ResultSet | RowModifying | SimpleResult)`
- `Rows` / `Row` / `Field` — result data. `Field.value` is `FieldData` (open interface)
- `FieldData` — `interface val` requiring `fun string(): String iso^`. Open result type for decoded column values. All built-in types conform structurally. Custom codecs return their own types implementing this interface
- `FieldDataEquatable` — `interface val` with `fun field_data_eq(that: FieldData box): Bool`. Opt-in equality for custom `FieldData` types used in `Field.eq()`. Built-in types use explicit match arms; custom types implement this to participate in field equality
- `_FieldDataEq` — package-private primitive with shared equality logic for `FieldData` values. Used by `Field.eq()` and `PgArray.eq()`. Methods: `apply(a, b)`, `nullable(a, b)`
- `Bytea` — val class wrapping `Array[U8] val` for PostgreSQL bytea columns (OID 17). Access raw bytes via `.data`. Implements `FieldData` and `Equatable[Bytea]`
- `RawBytes` — val class wrapping `Array[U8] val` for unknown binary-format OIDs. Semantically distinct from `Bytea`. Implements `FieldData` and `Equatable[RawBytes]`
- `FieldDataTypes` = `(Array[U8] val | Bool | F32 | F64 | I16 | I32 | I64 | None | PgArray | PgDate | PgInterval | PgTime | PgTimestamp | String)` — closed union for encode path (query parameters)
- `PgTimestamp` — val class wrapping `microseconds: I64` (since 2000-01-01). `I64.max_value()` / `I64.min_value()` = infinity. Implements `Equatable`, `string()` returns ISO format. For `timestamptz` columns: binary-format results (`PreparedQuery`) store UTC microseconds; text-format results (`SimpleQuery`) store session-local time with timezone suffix stripped
- `PgTimeMicroseconds` — type alias for `Constrained[I64, PgTimeValidator]`. Validated microseconds value for constructing `PgTime`
- `MakePgTimeMicroseconds` — type alias for `MakeConstrained[I64, PgTimeValidator]`. Returns `(PgTimeMicroseconds | ValidationFailure)`
- `PgTimeValidator` — primitive implementing `Validator[I64]`, validates `[0, 86,400,000,000)`
- `PgTime` — val class wrapping `microseconds: I64` (since midnight). Constructor takes `PgTimeMicroseconds` (non-partial). Implements `Equatable`, `string()` returns `HH:MM:SS[.fractional]`
- `PgDate` — val class wrapping `days: I32` (since 2000-01-01). `I32.max_value()` / `I32.min_value()` = infinity. Implements `Equatable`, `string()` returns ISO date
- `PgInterval` — val class wrapping `microseconds: I64`, `days: I32`, `months: I32`. Implements `Equatable`, `string()` returns PostgreSQL interval format
- `PgArray` — val class wrapping `element_oid: U32` and `elements: Array[(FieldData | None)] val`. Methods: `create`, `size`, `apply(i) ?`, `eq`, `field_data_eq`, `string` (PostgreSQL array literal format). Implements `FieldData & FieldDataEquatable & Equatable[PgArray]`
- `_TemporalFormat` — package-private primitive with shared date/time formatting helpers (`pg_epoch_jdn`, `days_to_date`, `append_date`, `append_two_digits`, `append_fractional`). Used by all 4 temporal types and text codecs
- `TransactionStatus` — union type `(TransactionIdle | TransactionInBlock | TransactionFailed)`. Reported via `pg_transaction_status` callback on every `ReadyForQuery`.
- `Notification` — val class wrapping channel name, payload string, and notifying backend's process ID. Delivered via `pg_notification` callback.
- `NoticeResponseMessage` — non-fatal PostgreSQL notice with all standard fields (same structure as `ErrorResponseMessage`). Delivered via `pg_notice` callback.
- `ParameterStatus` — val class wrapping a runtime parameter name and value reported by the server. Delivered via `pg_parameter_status` callback during startup and after SET commands.
- `SessionStatusNotify` interface (tag) — lifecycle callbacks (connected, connection_failed(reason), authenticated, authentication_failed, transaction_status, notification, notice, parameter_status, shutdown)
- `ResultReceiver` interface (tag) — `pg_query_result(Session, Result)`, `pg_query_failed(Session, Query, (ErrorResponseMessage | ClientQueryError))`
- `PrepareReceiver` interface (tag) — `pg_statement_prepared(Session, name)`, `pg_prepare_failed(Session, name, (ErrorResponseMessage | ClientQueryError))`
- `CopyInReceiver` interface (tag) — `pg_copy_ready(Session)`, `pg_copy_complete(Session, count)`, `pg_copy_failed(Session, (ErrorResponseMessage | ClientQueryError))`. Pull-based: session calls `pg_copy_ready` after `copy_in` and after each `send_copy_data`, letting the client control data flow
- `CopyOutReceiver` interface (tag) — `pg_copy_data(Session, Array[U8] val)`, `pg_copy_complete(Session, count)`, `pg_copy_failed(Session, (ErrorResponseMessage | ClientQueryError))`. Push-based: server drives the flow, delivering data chunks via `pg_copy_data` and signaling completion via `pg_copy_complete`
- `StreamingResultReceiver` interface (tag) — `pg_stream_batch(Session, Rows)`, `pg_stream_complete(Session)`, `pg_stream_failed(Session, (PreparedQuery | NamedPreparedQuery), (ErrorResponseMessage | ClientQueryError))`. Pull-based: session delivers batches via `pg_stream_batch`; client calls `fetch_more()` for the next batch or `close_stream()` to end early
- `PipelineReceiver` interface (tag) — `pg_pipeline_result(Session, USize, Result)`, `pg_pipeline_failed(Session, USize, (PreparedQuery | NamedPreparedQuery), (ErrorResponseMessage | ClientQueryError))`, `pg_pipeline_complete(Session)`. Each query result/failure is delivered with its pipeline index. `pg_pipeline_complete` always fires last
- `Codec` interface (val) — `format(): U16`, `encode(FieldDataTypes): Array[U8] val ?`, `decode(Array[U8] val): FieldData ?`. Wire format codec for a PostgreSQL type. Encode stays closed (`FieldDataTypes`), decode is open (`FieldData`). Built-in codecs are primitives (zero-allocation singletons)
- `CodecRegistry` class (val) — maps OIDs to text and binary `Codec` instances. Immutable — `with_codec(oid, codec)?` returns a new registry with the codec registered (partial — errors if OID is already registered, built-in or custom, or collides with a built-in or custom array OID). `with_enum_type(oid)?` registers text-passthrough codecs for both text and binary formats atomically (partial — same validation as `with_codec` but checks both codec maps; after `with_enum_type(X)`, `with_codec(X, ...)` errors for either format). `with_array_type(array_oid, element_oid)?` registers a custom array type mapping (partial — errors if element_oid is an array OID, array_oid collides with a registered OID, array_oid is already a custom array OID, array_oid is already a custom element OID, or array_oid == element_oid). `array_oid_for(element_oid)` returns the array OID (built-in + custom, 0 if unknown). Supports chaining: `CodecRegistry.with_enum_type(12345)?.with_array_type(12350, 12345)?`. Default constructor populates all built-in codecs. `decode(oid, format, data)` is partial — returns `FieldData` for known OIDs, fallbacks for unknown OIDs (unknown text→`String`, unknown binary→`RawBytes`), and errors when a registered codec's `decode()` fails. Intercepts array OIDs before normal codec dispatch. `has_binary_codec(oid)` for format selection (includes array OIDs)
- `ClientQueryError` — union type `(SessionNeverOpened | SessionClosed | SessionNotAuthenticated | DataError)`
- `DatabaseConnectInfo` — val class grouping database authentication parameters (user, password, database). Passed to `Session.create()` alongside `ServerConnectInfo`.
- `ServerConnectInfo` — val class grouping connection parameters (auth, host, service, ssl_mode, connection_timeout). Passed to `Session.create()` as the first parameter. Also used by `_CancelSender`. Optional `connection_timeout: (lori.ConnectionTimeout | None)` bounds the TCP connection phase; if the timeout fires before a connection is established, `pg_session_connection_failed` is called with `ConnectionFailedTimeout`.
- `SSLMode` — union type `(SSLDisabled | SSLPreferred | SSLRequired)`. `SSLDisabled` is the default (plaintext). `SSLPreferred` wraps an `SSLContext val` and attempts SSL with plaintext fallback on server refusal (`sslmode=prefer`). `SSLRequired` wraps an `SSLContext val` and aborts on server refusal.
- `ErrorResponseMessage` — full PostgreSQL error with all standard fields
- `AuthenticationFailureReason` = `(InvalidAuthenticationSpecification | InvalidPassword | ServerVerificationFailed | UnsupportedAuthenticationMethod)`
- `ConnectionFailureReason` = `(ConnectionFailedDNS | ConnectionFailedTCP | SSLServerRefused | TLSAuthFailed | TLSHandshakeFailed | ConnectionFailedTimeout)`. Delivered via `pg_session_connection_failed` callback

### Type Conversion (PostgreSQL OID → Pony)

Codec-based decoding via `CodecRegistry.decode(oid, format_code, data)`. Extended query results use binary format (format_code=1); SimpleQuery results use text format (format_code=0).

**Binary format codecs** (extended query path):
- 16 (bool) → `Bool`, 17 (bytea) → `Bytea`
- 21 (int2) → `I16`, 23 (int4) → `I32`, 20 (int8) → `I64`
- 700 (float4) → `F32`, 701 (float8) → `F64`
- 1082 (date) → `PgDate`, 1083 (time) → `PgTime`
- 1114 (timestamp) / 1184 (timestamptz) → `PgTimestamp`
- 1186 (interval) → `PgInterval`
- 26 (oid) → `String`, 1700 (numeric) → `String`, 2950 (uuid) → `String`, 3802 (jsonb) → `String`
- 18 (char), 19 (name), 25 (text), 114 (json), 142 (xml), 1042 (bpchar), 1043 (varchar) → `String` (text passthrough binary codec)
- Unknown OIDs → `RawBytes`
- NULL → `None`

**Text format codecs** (SimpleQuery path): Same type mappings as binary codecs. Unknown OIDs → `String`.

**Fallbacks and errors**: `CodecRegistry.decode()` is partial. For unknown OIDs (no registered codec), it returns fallbacks: `String` for text format, `RawBytes` for binary format. For known OIDs where the registered codec's `decode()` errors, the error propagates to the caller. For arrays, structural parsing errors (malformed wire format) fall back, but element codec errors propagate — this distinguishes unrecognized array formats from corrupt element data.

**Array OIDs** (element OID → array OID, 23 built-in mappings): 16→1000 (bool[]), 17→1001 (bytea[]), 18→1002 (char[]), 19→1003 (name[]), 20→1016 (int8[]), 21→1005 (int2[]), 23→1007 (int4[]), 25→1009 (text[]), 26→1028 (oid[]), 114→199 (json[]), 142→143 (xml[]), 700→1021 (float4[]), 701→1022 (float8[]), 1042→1014 (bpchar[]), 1043→1015 (varchar[]), 1082→1182 (date[]), 1083→1183 (time[]), 1114→1115 (timestamp[]), 1184→1185 (timestamptz[]), 1186→1187 (interval[]), 1700→1231 (numeric[]), 2950→2951 (uuid[]), 3802→3807 (jsonb[]). Arrays decoded via `CodecRegistry` interception (before per-OID codec dispatch). Binary format produces `PgArray`; text format produces `PgArray`.

### Codec Architecture

`Codec` interface with `encode`/`decode`/`format` methods. Built-in codecs are primitives:
- Binary codecs (`_binary_codecs.pony`): `_BoolBinaryCodec`, `_ByteaBinaryCodec`, `_Int2BinaryCodec`, `_Int4BinaryCodec`, `_Int8BinaryCodec`, `_Float4BinaryCodec`, `_Float8BinaryCodec`, `_DateBinaryCodec`, `_TimeBinaryCodec`, `_TimestampBinaryCodec`, `_IntervalBinaryCodec`, `_OidBinaryCodec`, `_NumericBinaryCodec` (encode implemented: parses decimal string → base-10000 digit format; handles positive/negative integers, fractional values, zero, NaN, Infinity/-Infinity), `_UuidBinaryCodec`, `_JsonbBinaryCodec` — big-endian wire encoding
- Text passthrough binary codec (`_text_passthrough_binary_codec.pony`): `_TextPassthroughBinaryCodec` — for text-like OIDs (char, name, text, json, xml, bpchar, varchar) where PostgreSQL binary format is raw UTF-8
- Text codecs (`_text_codecs.pony`): `_BoolTextCodec`, `_ByteaTextCodec`, `_Int2TextCodec`, `_Int4TextCodec`, `_Int8TextCodec`, `_Float4TextCodec`, `_Float8TextCodec`, `_DateTextCodec`, `_TimeTextCodec`, `_TimestampTextCodec`, `_TimestamptzTextCodec`, `_IntervalTextCodec` (supports all four `intervalstyle` formats: `postgres`, `postgres_verbose`, `iso_8601`, `sql_standard` — detected via heuristic in `decode()`), `_TextPassthroughTextCodec`, `_OidTextCodec`, `_NumericTextCodec`, `_UuidTextCodec`, `_JsonbTextCodec`
- `_ArrayOidMap` (`_array_oid_map.pony`): static bidirectional mapping between element OIDs and array OIDs (23 entries). Methods: `element_oid_for(array_oid)`, `array_oid_for(element_oid)`, `is_array_oid(oid)`
- `_ArrayEncoder` (`_array_encoder.pony`): encodes `PgArray` to binary array wire format. Dispatches element encoding on Pony runtime types. String elements routed by `element_oid`: uuid → `_UuidBinaryCodec`, jsonb → `_JsonbBinaryCodec`, oid → `_OidBinaryCodec`, numeric → `_NumericBinaryCodec`; all others → raw UTF-8 bytes. Coupling: element encoding must stay in sync with `_FrontendMessage.bind()` and `_binary_codecs.pony`
- `CodecRegistry` (`codec_registry.pony`): maps OIDs to codecs. Adds `_custom_array_element_oids: Map[U32, U32] val` field for custom array type registrations. Default constructor populates all built-ins. `with_codec(oid, codec)?` returns a new registry with the codec registered (errors if OID already registered). `with_enum_type(oid)?` registers text-passthrough codecs for both text and binary formats atomically (errors if OID already registered in either codec map or collides with an array OID). `with_array_type(array_oid, element_oid)?` returns a new registry with the custom array mapping added (errors if element_oid is an array OID, array_oid collides with a registered OID, array_oid is already a custom array OID, array_oid is already a custom element OID, or array_oid == element_oid). `array_oid_for(element_oid): U32` returns the array OID for the given element OID. `decode()` intercepts array OIDs before normal codec dispatch via two-phase array decoding: `_parse_binary_array`/`_parse_text_array` extract structure and raw element bytes (errors fall back), then `_decode_array_elements` decodes each element via the element codec (errors propagate). `has_binary_codec()` checks array OIDs too. `_with_codec` constructor (type-private) used internally by `with_codec`; `_with_enum_type` constructor used internally by `with_enum_type`
- `_ParamEncoder` (`_param_encoder.pony`): derives PostgreSQL OIDs from `FieldDataTypes` parameter values for Parse messages. Takes `registry: CodecRegistry` parameter; `PgArray` arm uses `registry.array_oid_for(a.element_oid)`

**Encode error handling:** `_FrontendMessage.bind()` is partial — it errors if parameter encoding fails. Takes `registry: CodecRegistry` parameter (with default `CodecRegistry`); pre-encodes `PgArray` parameters via `_ArrayEncoder` before the `recover val` block. `_QueryReady.try_run_query()` uses a build-before-transition pattern: wire messages are constructed before transitioning to an in-flight state, so encode errors deliver `DataError` to the receiver without leaving the state machine inconsistent. Pipeline queries build message parts into an `iso` array in `ref` scope (where error handling has full access to the session and receiver), then consume the array into a `recover val` block for concatenation.

### Query Cancellation

`_CancelSender` actor — fire-and-forget actor that sends a `CancelRequest` on a separate TCP connection. PostgreSQL requires cancel requests on a different connection from the one executing the query. No response is expected on the cancel connection — the result (if any) arrives as an `ErrorResponse` on the original session connection. When the session uses `SSLRequired` or `SSLPreferred`, the cancel connection performs SSL negotiation before sending the CancelRequest — mirroring the main session's connection setup. For `SSLRequired`, if the server refuses SSL or the TLS handshake fails, the cancel is silently abandoned. For `SSLPreferred`, server refusal falls back to a plaintext cancel; TLS handshake failure still silently abandons. Created by `_SessionLoggedIn.cancel()` using the session's `ServerConnectInfo`, `backend_pid`, and `backend_secret_key`. Design: [discussion #88](https://github.com/ponylang/postgres/discussions/88).

### Mort Primitives

`_IllegalState` and `_Unreachable` in `_mort.pony`. Print file/line to stderr via FFI and exit. Issue URL: `https://github.com/ponylang/postgres/issues`.

## Test Organization

Tests live in the main `postgres/` package (private test classes), organized across multiple files by concern (`_test_*.pony`). The `Main` test actor in `_test.pony` is the single test registry that lists all tests. Read the individual test files for per-test details.

**Conventions**: `_test.pony` contains shared helpers (`_ConnectionTestConfiguration` for env vars, `_ConnectTestNotify`/`_AuthenticateTestNotify` reused by other files). `_test_response_parser.pony` contains `_Incoming*TestMessage` builder classes that construct raw protocol bytes for mock servers across all test files. `_test_mock_message_reader.pony` contains `_MockMessageReader` for extracting complete PostgreSQL frontend messages from TCP data in mock servers. `_test_codecs.pony` contains unit tests for binary/text codecs, `CodecRegistry`, `_ParamEncoder`, and binary-format bind wire format. `_test_array.pony` contains unit tests for `_ArrayOidMap`, binary/text array decode, `PgArray` equality/string, `_ArrayEncoder` roundtrips, `_ParamEncoder` with `PgArray`, `_FrontendMessage.bind()` with `PgArray`, `_FieldDataEq` extraction, `_NumericBinaryCodec` encode roundtrip, `CodecRegistry` array methods, and integration tests for array SELECT/roundtrip. `_test_statement_timeout.pony` contains unit tests for statement timeout timer lifecycle (fires on slow query, cancelled on normal completion) and integration test for pg_sleep with timeout. `_test_connection_timeout.pony` contains unit test for connection timeout (connects to non-routable address, verifies `ConnectionFailedTimeout` is reported).

**Ports**: Mock server tests use ports in the 7669–7723 range and 9667–9668. **Port 7680 is reserved by Windows** (Update Delivery Optimization) and will fail to bind on WSL2 — do not use it.

## Supported PostgreSQL Features

**SSL/TLS:** Optional SSL negotiation via `SSLRequired` (mandatory) or `SSLPreferred` (fallback to plaintext on server refusal). CVE-2021-23222 mitigated via `buffer_until(BufferSize)` before SSLRequest. Design: [discussion #76](https://github.com/ponylang/postgres/discussions/76).

**Authentication:** Cleartext password, MD5 password, and SCRAM-SHA-256. No SCRAM-SHA-256-PLUS (channel binding), Kerberos, GSS, or certificate auth. Design: [discussion #83](https://github.com/ponylang/postgres/discussions/83).

**Protocol:** Simple query and extended query (parameterized via unnamed and named prepared statements). Parameters use per-parameter format codes: binary encoding for typed values (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`, `PgTimestamp`, `PgTime`, `PgDate`, `PgInterval`, `PgArray`) with explicit OIDs in Parse messages; text format with server-inferred types for `String` and `None`. Extended query results use all-binary format (Bind message specifies `num_result_formats=1, format_code=1`); SimpleQuery results use text format. `_RowsBuilder` delegates decoding to `CodecRegistry` which selects binary or text codec based on the per-column format code from `_RowDescriptionMessage`. LISTEN/NOTIFY, NoticeResponse, ParameterStatus, COPY FROM STDIN (pull-based), COPY TO STDOUT (push-based), row streaming (portal-based cursors with windowed batch delivery via Execute(max_rows)+Flush+PortalSuspended), query pipelining (multiple Parse/Bind/Execute/Sync cycles in a single TCP write with per-query error isolation), 1-dimensional array types (decode and encode via `PgArray`), user-defined enum types (decode via `CodecRegistry.with_enum_type`), statement timeout (per-query automatic cancellation via CancelRequest). No function calls. Full feature roadmap: [discussion #72](https://github.com/ponylang/postgres/discussions/72). Codec design: [discussion #139](https://github.com/ponylang/postgres/discussions/139).

**CI containers:** Stock `postgres:14.5` for plain (port 5432, SCRAM-SHA-256 default) and `ghcr.io/ponylang/postgres-ci-pg-ssl:latest` for SSL (port 5433, SSL + md5user); built via `build-ci-image.yml` workflow dispatch or locally via `.ci-dockerfiles/pg-ssl/build-and-push.bash`. MD5 integration tests connect to the SSL container (without using SSL) because only that container has the md5user.

## PostgreSQL Wire Protocol Reference

For protocol details (message formats, extended query protocol, type OIDs, message type bytes), see the [PostgreSQL protocol documentation](https://www.postgresql.org/docs/14/protocol.html). Key pages: [message formats](https://www.postgresql.org/docs/14/protocol-message-formats.html), [message flow](https://www.postgresql.org/docs/14/protocol-flow.html).
