# Project: ponylang/postgres

Pure Pony PostgreSQL driver. Alpha-level. Version 0.2.2.

## Building and Testing

```
make ssl=3.0.x                        # build and run all tests
make unit-tests ssl=3.0.x             # unit tests only (no postgres needed)
make integration-tests ssl=3.0.x      # integration tests (needs postgres)
make build-examples ssl=3.0.x         # compile examples
make start-pg-container               # docker postgres:14.5 on port 5432
make stop-pg-container                # stop docker container
```

SSL version is mandatory. Tests run with `--sequential`. Integration tests require a running PostgreSQL 14.5 with MD5 auth (user: postgres, password: postgres, database: postgres). Environment variables: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`.

## Dependencies

- `ponylang/ssl` 1.0.1 (MD5 password hashing via `ssl/crypto`)
- `ponylang/lori` 0.6.2 (TCP networking)

Managed via `corral`.

## GitHub Labels

- `changelog - added`, `changelog - changed`, `changelog - fixed` — **PR-only labels**. CI uses these to auto-generate CHANGELOG entries on merge. Never apply to issues.
- `bug`, `help wanted`, `good first issue`, `documentation`, etc. — issue classification labels.

## Architecture

### Session State Machine

`Session` actor is the main entry point. Implements `lori.TCPConnectionActor` and `lori.ClientLifecycleEventReceiver`. State transitions via `_SessionState` interface with four concrete states:

```
_SessionUnopened  --connect-->    _SessionConnected
_SessionUnopened  --fail-->       _SessionClosed
_SessionConnected --auth ok-->    _SessionLoggedIn
_SessionConnected --auth fail-->  _SessionClosed
_SessionLoggedIn  --close-->      _SessionClosed
```

State behavior is composed via a trait hierarchy that mixes in capabilities and defaults:
- `_ConnectableState` / `_NotConnectableState` — can/can't receive connection events
- `_ConnectedState` / `_UnconnectedState` — has/doesn't have a live connection
- `_AuthenticableState` / `_NotAuthenticableState` — can/can't authenticate
- `_AuthenticatedState` / `_NotAuthenticated` — has/hasn't authenticated

This design makes illegal state transitions call `_IllegalState()` (panic) by default via the trait hierarchy, so only valid transitions need explicit implementation.

### Query Execution Flow

1. Client calls `session.execute(query, ResultReceiver)` where query is `SimpleQuery` or `PreparedQuery`
2. `_SessionLoggedIn` queues `(query, receiver)` pairs in `query_queue`
3. The `_QueryState` sub-state machine manages query lifecycle:
   - `_QueryNotReady`: initial state after auth, waiting for server readiness
   - `_QueryReady`: server is idle, `try_run_query` dispatches based on query type — `SimpleQuery` transitions to `_SimpleQueryInFlight`, `PreparedQuery` transitions to `_ExtendedQueryInFlight`
   - `_SimpleQueryInFlight`: owns per-query accumulation data (`_data_rows`, `_row_description`), delivers results on `CommandComplete`
   - `_ExtendedQueryInFlight`: same data accumulation and result delivery as `_SimpleQueryInFlight` (duplicated because Pony traits can't have iso fields). Entered after sending Parse+Bind+Describe(portal)+Execute+Sync
4. Response data arrives: `_RowDescriptionMessage` sets column metadata, `_DataRowMessage` accumulates rows
5. `_CommandCompleteMessage` triggers result delivery to receiver
6. `_ReadyForQueryMessage` dequeues completed query, transitions to `_QueryReady` (idle) or `_QueryNotReady` (non-idle)

Only one query is in-flight at a time. The queue serializes execution. `query_queue` and `query_state` are non-underscore-prefixed fields on `_SessionLoggedIn` because the `_QueryState` implementations need cross-type access (Pony private fields are type-private).

### Protocol Layer

**Frontend (client → server):**
- `_FrontendMessage` primitive: `startup()`, `password()`, `query()`, `parse()`, `bind()`, `describe_portal()`, `execute_msg()`, `sync()` — builds raw byte arrays with big-endian wire format

**Backend (server → client):**
- `_ResponseParser` primitive: incremental parser consuming from a `Reader` buffer. Returns one parsed message per call, `None` if incomplete, errors on junk.
- `_ResponseMessageParser` primitive: routes parsed messages to the current session state's callbacks. Processes messages synchronously within a query cycle (looping until `ReadyForQuery` or buffer exhaustion), then yields via `s._process_again()` between cycles. This prevents behaviors like `close()` from interleaving between result delivery and query dequeuing. If a callback triggers shutdown during the loop, `on_shutdown` clears the read buffer, causing the next parse to return `None` and exit the loop.

**Supported message types:** AuthenticationOk, AuthenticationMD5Password, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, ReadyForQuery, RowDescription, ParseComplete, BindComplete, NoData, CloseComplete, ParameterDescription, PortalSuspended. Extended query acknowledgment messages (ParseComplete, BindComplete, NoData, etc.) are parsed but silently consumed — they fall through the `_ResponseMessageParser` match without routing since the state machine tracks query lifecycle through data-carrying messages only.

### Public API Types

- `Query` — union type `(SimpleQuery | PreparedQuery)`
- `SimpleQuery` — val class wrapping a query string (simple query protocol)
- `PreparedQuery` — val class wrapping a query string + `Array[(String | None)] val` params (extended query protocol, single statement only)
- `Result` trait — `ResultSet` (rows), `SimpleResult` (no rows), `RowModifying` (INSERT/UPDATE/DELETE with count)
- `Rows` / `Row` / `Field` — result data. `Field.value` is `FieldDataTypes` union
- `FieldDataTypes` = `(Bool | F32 | F64 | I16 | I32 | I64 | None | String)`
- `SessionStatusNotify` interface (tag) — lifecycle callbacks (connected, connection_failed, authenticated, authentication_failed, shutdown)
- `ResultReceiver` interface (tag) — `pg_query_result(Result)`, `pg_query_failed(Query, (ErrorResponseMessage | ClientQueryError))`
- `ClientQueryError` trait — `SessionNeverOpened`, `SessionClosed`, `SessionNotAuthenticated`, `DataError`
- `ErrorResponseMessage` — full PostgreSQL error with all standard fields
- `AuthenticationFailureReason` = `(InvalidAuthenticationSpecification | InvalidPassword)`

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

### Mort Primitives

`_IllegalState` and `_Unreachable` in `_mort.pony`. Print file/line to stderr via FFI and exit. Issue URL: `https://github.com/ponylang/postgres/issues`.

## Test Organization

Tests live in the main `postgres/` package (private test classes).

**Unit tests** (no external dependencies):
- `_TestFrontendMessage*` — verify wire format of outgoing messages
- `_TestResponseParser*` — verify parsing of individual and sequential backend messages
- `_TestHandlingJunkMessages` — uses a local TCP listener that sends junk; verifies session shuts down
- `_TestUnansweredQueriesFailOnShutdown` — uses a local TCP listener that auto-auths but never responds to queries; verifies queued queries get `SessionClosed` failures

**Integration tests** (require PostgreSQL, names prefixed `integration/`):
- Connect, ConnectFailure, Authenticate, AuthenticateFailure
- Query/Results, Query/AfterAuthenticationFailure, Query/AfterConnectionFailure, Query/AfterSessionHasBeenClosed
- Query/OfNonExistentTable, Query/CreateAndDropTable, Query/InsertAndDelete, Query/EmptyQuery
- PreparedQuery/Results, PreparedQuery/NullParam, PreparedQuery/OfNonExistentTable
- PreparedQuery/InsertAndDelete, PreparedQuery/MixedWithSimple

Test helpers: `_ConnectionTestConfiguration` reads env vars with defaults. Several test message builder classes (`_Incoming*TestMessage`) construct raw protocol bytes for unit tests.

## Known Issues and TODOs in Code

- `rows.pony:43` — TODO: need tests for Rows/Row/Field (requires implementing `eq`)
- `_test_response_parser.pony:6` — TODO: chain-of-messages tests to verify correct buffer advancement across message sequences
- `result_receiver.pony:1` — TODO: consider passing session to result callbacks so receivers without a session tag can execute follow-up queries

## Roadmap

Next priority: **named prepared statements** (current implementation uses unnamed statements only — each PreparedQuery creates and immediately executes a fresh statement).

## Supported PostgreSQL Features

**Authentication:** MD5 password only. No SCRAM-SHA-256, Kerberos, SASL, GSS, or certificate auth.

**Protocol:** Simple query protocol and extended query protocol (parameterized queries via unnamed prepared statements). Parameters are text-format only; type OIDs are inferred by the server. No named prepared statements, COPY, LISTEN/NOTIFY, or function calls.

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

**Frontend**: `Q`=Query, `P`=Parse, `B`=Bind, `D`=Describe, `E`=Execute, `C`=Close, `S`=Sync, `H`=Flush, `X`=Terminate, `p`=PasswordMessage

**Backend**: `R`=Auth, `K`=BackendKeyData, `S`=ParameterStatus, `Z`=ReadyForQuery, `T`=RowDescription, `D`=DataRow, `C`=CommandComplete, `I`=EmptyQueryResponse, `1`=ParseComplete, `2`=BindComplete, `3`=CloseComplete, `t`=ParameterDescription, `n`=NoData, `s`=PortalSuspended, `E`=ErrorResponse, `N`=NoticeResponse, `A`=NotificationResponse

## File Layout

```
postgres/                         # Main package (26 files)
  session.pony                    # Session actor + state machine traits + query sub-state machine
  simple_query.pony               # SimpleQuery class
  prepared_query.pony             # PreparedQuery class
  query.pony                      # Query union type
  result.pony                     # Result, ResultSet, SimpleResult, RowModifying
  result_receiver.pony            # ResultReceiver interface
  session_status_notify.pony      # SessionStatusNotify interface
  query_error.pony                # ClientQueryError types
  error_response_message.pony     # ErrorResponseMessage + builder
  field.pony                      # Field class
  field_data_types.pony           # FieldDataTypes union
  row.pony                        # Row class
  rows.pony                       # Rows, RowIterator, _RowsBuilder
  _frontend_message.pony          # Client-to-server messages
  _backend_messages.pony          # Server-to-client message types
  _message_type.pony              # Protocol message type codes
  _response_parser.pony           # Wire protocol parser
  _response_message_parser.pony   # Routes parsed messages to session state
  _authentication_request_type.pony
  _authentication_failure_reason.pony
  _md5_password.pony              # MD5 password construction
  _mort.pony                      # Panic primitives
  _test.pony                      # Main test actor + integration tests
  _test_query.pony                # Query integration tests
  _test_response_parser.pony      # Parser unit tests + test message builders
  _test_frontend_message.pony     # Frontend message unit tests
examples/README.md                # Examples overview
examples/query/query-example.pony # Simple query with result inspection
examples/prepared-query/prepared-query-example.pony # PreparedQuery with params and NULL
examples/crud/crud-example.pony   # Multi-query CRUD workflow
```
