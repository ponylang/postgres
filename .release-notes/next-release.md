## Update ponylang/ssl dependency to 1.0.1

We've updated the ponylang/ssl library dependency in this project to 1.0.1.

## Fix typo in SesssionNeverOpened

`SesssionNeverOpened` has been renamed to `SessionNeverOpened`.

Before:

```pony
match error
| SesssionNeverOpened => "session never opened"
end
```

After:

```pony
match error
| SessionNeverOpened => "session never opened"
end
```

## Fix ErrorResponseMessage routine field never being populated

The error response parser incorrectly mapped the `'R'` (Routine) protocol field to `line` instead of `routine` on `ErrorResponseMessage`. The `routine` field was never populated as a result. It now correctly contains the name of the source-code routine that reported the error.

## Fix zero-row SELECT producing RowModifying instead of ResultSet

A `SELECT` query returning zero rows (e.g., `SELECT 1 WHERE false`) incorrectly produced a `RowModifying` result instead of a `ResultSet` with zero rows. This made it impossible to distinguish a zero-row SELECT from an INSERT/UPDATE/DELETE at the result level. Zero-row SELECTs now correctly produce a `ResultSet`.

## Fix double-delivery of pg_query_failed on failed transactions

When a query error occurred inside a PostgreSQL transaction, the `ResultReceiver` could receive `pg_query_failed` twice for the same query — once with the original error, and again with `SessionClosed` if `close()` was called before the session became idle. The errored query now correctly completes after `ReadyForQuery` regardless of transaction status.

## Fix double-delivery of pg_query_failed when close() races with error processing

When `close()` was called while the session was between processing an error response and processing the subsequent ready-for-query message, the `ResultReceiver` could receive `pg_query_failed` twice — once with the original error and again with `SessionClosed`. Query cycle messages are now processed synchronously, preventing other operations from interleaving.

## Add parameterized queries via extended query protocol

You can now execute parameterized queries using `PreparedQuery`. Parameters are referenced as `$1`, `$2`, etc. in the query string and passed as an `Array[FieldDataTypes] val`. Typed values (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`, `PgTimestamp`, `PgTime`, `PgDate`, `PgInterval`) use binary wire format; `String` and `None` use text format. Use `None` for SQL NULL.

```pony
// Parameterized SELECT with typed parameter
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as FieldDataTypes: I32(42)] end)
session.execute(query, receiver)

// INSERT with NULL parameter
let insert = PreparedQuery("INSERT INTO items (name, desc) VALUES ($1, $2)",
  recover val [as FieldDataTypes: "widget"; None] end)
session.execute(insert, receiver)
```

Each `PreparedQuery` must contain a single SQL statement. For multi-statement execution, use `SimpleQuery`.

## Change ResultReceiver and Result to use Query union type

`ResultReceiver.pg_query_failed` and `Result.query()` now use `Query` (a union of `SimpleQuery | PreparedQuery | NamedPreparedQuery`) instead of `SimpleQuery`.

Before:

```pony
be pg_query_failed(query: SimpleQuery,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  // handle failure
```

After:

```pony
be pg_query_failed(session: Session, query: Query,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  match query
  | let sq: SimpleQuery => // ...
  | let pq: PreparedQuery => // ...
  | let nq: NamedPreparedQuery => // ...
  end
```

## Add named prepared statement support

You can now create server-side named prepared statements with `Session.prepare()`, execute them with `NamedPreparedQuery`, and destroy them with `Session.close_statement()`. Named statements are parsed once and can be executed multiple times with different parameters, avoiding repeated parsing overhead.

```pony
// Prepare a named statement
session.prepare("find_user", "SELECT * FROM users WHERE id = $1", receiver)

// In the PrepareReceiver callback:
be pg_statement_prepared(session: Session, name: String) =>
  // Execute with different parameters
  session.execute(
    NamedPreparedQuery("find_user",
      recover val [as FieldDataTypes: I32(42)] end),
    result_receiver)

// Clean up when done
session.close_statement("find_user")
```

The `Query` union type now includes `NamedPreparedQuery`, so exhaustive matches on `Query` need a new branch:

```pony
match query
| let sq: SimpleQuery => sq.string
| let pq: PreparedQuery => pq.string
| let nq: NamedPreparedQuery => nq.name
end
```

## Add SSL/TLS negotiation support

You can now encrypt connections to PostgreSQL using SSL/TLS. Pass `SSLRequired(sslctx)` via `ServerConnectInfo` to `Session.create()` to enable SSL negotiation before authentication. The default `SSLDisabled` preserves the existing plaintext behavior.

```pony
use "ssl/net"
use "postgres"

// Create an SSLContext (configure certificates/verification as needed)
let sslctx = recover val
  SSLContext
    .> set_client_verify(false)
    .> set_server_verify(false)
end

// Connect with SSL
let session = Session(
  ServerConnectInfo(auth, host, port, SSLRequired(sslctx)),
  DatabaseConnectInfo(username, password, database),
  notify)
```

If the server accepts SSL, the connection is encrypted before authentication begins. If the server refuses, `pg_session_connection_failed` fires.

## Change ResultReceiver and PrepareReceiver callbacks to take Session as first parameter

All `ResultReceiver` and `PrepareReceiver` callbacks now take `Session` as their first parameter, matching the convention used by `SessionStatusNotify`. This enables receivers to execute follow-up queries directly from callbacks without storing a session reference (see "Enable follow-up queries from ResultReceiver and PrepareReceiver callbacks" below).

Before:

```pony
be pg_query_result(result: Result) =>
  // ...

be pg_query_failed(query: Query,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  // ...

be pg_statement_prepared(name: String) =>
  // ...

be pg_prepare_failed(name: String,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  // ...
```

After:

```pony
be pg_query_result(session: Session, result: Result) =>
  // ...

be pg_query_failed(session: Session, query: Query,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  // ...

be pg_statement_prepared(session: Session, name: String) =>
  // ...

be pg_prepare_failed(session: Session, name: String,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  // ...
```

## Enable follow-up queries from ResultReceiver and PrepareReceiver callbacks

`ResultReceiver` and `PrepareReceiver` callbacks now receive the `Session`, so receivers can execute follow-up queries, close the session, or chain operations directly from callbacks without needing to store a session reference at construction time.

```pony
actor MyReceiver is ResultReceiver
  // no need to store session — it's passed to every callback

  be pg_query_result(session: Session, result: Result) =>
    // execute a follow-up query using the session from the callback
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    session.close()
```

## Add equality comparison for Field

`Field` now implements `Equatable`, enabling `==` and `!=` comparisons. A `Field` holds a column name and a typed value. Two fields are equal when they have the same name and the same value — the values must be the same type and compare equal using that type's own equality.

```pony
Field("id", I32(42)) == Field("id", I32(42))    // true
Field("id", I32(42)) == Field("id", I64(42))    // false — different types
Field("id", I32(42)) == Field("name", I32(42))  // false — different names
```

## Add equality comparison for Row

`Row` now implements `Equatable`, enabling `==` and `!=` comparisons. A `Row` holds an ordered sequence of `Field` values representing a single result row. Two rows are equal when they have the same number of fields and each corresponding pair of fields is equal. Field order matters — the same fields in a different order are not equal.

```pony
let r1 = Row(recover val [Field("id", I32(1)); Field("name", "Alice")] end)
let r2 = Row(recover val [Field("id", I32(1)); Field("name", "Alice")] end)
r1 == r2  // true

let r3 = Row(recover val [Field("name", "Alice"); Field("id", I32(1))] end)
r1 == r3  // false — same fields, different order
```

## Add equality comparison for Rows

`Rows` now implements `Equatable`, enabling `==` and `!=` comparisons. A `Rows` holds an ordered collection of `Row` values representing a query result set. Two `Rows` are equal when they have the same number of rows and each corresponding pair of rows is equal. Row order matters — the same rows in a different order are not equal.

```pony
let rs1 = Rows(recover val
  [Row(recover val [Field("id", I32(1))] end)]
end)
let rs2 = Rows(recover val
  [Row(recover val [Field("id", I32(1))] end)]
end)
rs1 == rs2  // true
```

## Send Terminate message before closing TCP connection

`Session.close()` now sends a Terminate message to the PostgreSQL server before closing the TCP connection. Previously, the connection was hard-closed without notifying the server, which could leave server-side resources (session state, prepared statements, temp tables) lingering until the server detected the broken connection on its next I/O attempt.

No code changes are needed — `Session.close()` handles this automatically.

## Add query cancellation support

You can now cancel a running query by calling `session.cancel()`. This sends a PostgreSQL CancelRequest on a separate connection, requesting the server to abort the in-flight query. Cancellation is best-effort — the server may or may not honor it. If cancelled, the query's `ResultReceiver` receives `pg_query_failed` with an `ErrorResponseMessage` containing SQLSTATE `57014` (query_canceled).

```pony
be pg_session_authenticated(session: Session) =>
  session.execute(SimpleQuery("SELECT pg_sleep(60)"), receiver)
  session.cancel()

be pg_query_failed(session: Session, query: Query,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  match failure
  | let err: ErrorResponseMessage =>
    if err.code == "57014" then
      // query was successfully cancelled
    end
  end
```

`cancel()` is safe to call at any time — it is a no-op if no query is in flight. When the session uses `SSLRequired`, the cancel connection uses SSL as well.

## Change Session constructor to accept ServerConnectInfo

`Session.create()` now takes a `ServerConnectInfo` as its first parameter instead of individual connection arguments. `ServerConnectInfo` groups auth, host, service, and SSL mode into a single immutable value.

Before:

```pony
let session = Session(
  auth, notify, host, port, username, password, database)
```

After:

```pony
let session = Session(
  ServerConnectInfo(auth, host, port),
  DatabaseConnectInfo(username, password, database),
  notify)
```

## Change Session constructor to accept DatabaseConnectInfo

`Session.create()` now takes a `DatabaseConnectInfo` instead of individual `user`, `password`, and `database` string parameters. `DatabaseConnectInfo` groups these authentication parameters into a single immutable value, matching the pattern established by `ServerConnectInfo`.

Before:

```pony
let session = Session(
  ServerConnectInfo(auth, host, port),
  notify,
  username,
  password,
  database)
```

After:

```pony
let session = Session(
  ServerConnectInfo(auth, host, port),
  DatabaseConnectInfo(username, password, database),
  notify)
```

## Add SCRAM-SHA-256 authentication support

The driver now supports SCRAM-SHA-256 authentication, which has been the default PostgreSQL authentication method since version 10. The authentication method is negotiated automatically — no code changes are needed. Existing code that connects to PostgreSQL servers using SCRAM-SHA-256 will now authenticate successfully where it previously failed.

Two new `AuthenticationFailureReason` variants are available for more specific error handling:

```pony
be pg_session_authentication_failed(
  session: Session,
  reason: AuthenticationFailureReason)
=>
  match reason
  | InvalidPassword => // wrong credentials
  | InvalidAuthenticationSpecification => // bad username
  | ServerVerificationFailed => // server's SCRAM signature didn't match (possible MITM)
  | UnsupportedAuthenticationMethod => // server requested an unsupported auth method
  end
```

MD5 authentication continues to work as before.

## Fix unsupported authentication type causing silent hang

When a PostgreSQL server requested an authentication method the driver doesn't support (e.g., Kerberos, GSSAPI), the session would hang indefinitely with no error reported. It now correctly fails with `UnsupportedAuthenticationMethod` via the `pg_session_authentication_failed` callback.

## Fix ReadyForQuery queue stall with explicit transactions

Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) caused the query queue to permanently stall. Any query following `BEGIN` would never execute because the driver incorrectly treated the server's "in transaction" status as "not ready for the next command."

Transactions now work as expected:

```pony
be pg_session_authenticated(session: Session) =>
  session.execute(SimpleQuery("BEGIN"), receiver)
  session.execute(SimpleQuery("INSERT INTO t (col) VALUES ('x')"), receiver)
  session.execute(SimpleQuery("COMMIT"), receiver)
```

## Add transaction status tracking

Every PostgreSQL `ReadyForQuery` message includes a transaction status byte. The new `pg_transaction_status` callback on `SessionStatusNotify` exposes this as a `TransactionStatus` union type, letting you track whether the session is idle, inside a transaction block, or in a failed transaction state.

```pony
actor Client is (SessionStatusNotify & ResultReceiver)
  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    match status
    | TransactionIdle => // not in a transaction
    | TransactionInBlock => // inside BEGIN...COMMIT/ROLLBACK
    | TransactionFailed => // error occurred, must ROLLBACK
    end
```

The callback fires after every query cycle completes, including the initial ready signal after authentication. Existing code is unaffected — the callback has a default no-op body.

## Add LISTEN/NOTIFY support

The driver now delivers PostgreSQL asynchronous notifications via a new `pg_notification` callback on `SessionStatusNotify`. Subscribe to a channel with `LISTEN` and receive notifications as they arrive from the server.

New types:

- `Notification` — a val class with `channel: String`, `payload: String`, and `pid: I32` fields
- `pg_notification(session, notification)` — a new behavior on `SessionStatusNotify` with a default no-op body (existing code is unaffected)

Usage:

```pony
actor MyClient is (SessionStatusNotify & ResultReceiver)
  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("LISTEN my_channel"), this)

  be pg_notification(session: Session, notification: Notification) =>
    env.out.print("Got: " + notification.channel + " -> " + notification.payload)
```

## Add COPY IN support

You can now bulk-load data into PostgreSQL using `COPY ... FROM STDIN`. Call `session.copy_in()` with a COPY SQL statement and a `CopyInReceiver` to start the operation. The driver uses a pull-based flow — it calls `pg_copy_ready` when ready for the next chunk, and the receiver responds with exactly one of `send_copy_data`, `finish_copy`, or `abort_copy`.

New types:

- `CopyInReceiver` — a tag interface with three callbacks: `pg_copy_ready`, `pg_copy_complete`, `pg_copy_failed`
- `Session.copy_in(sql, receiver)` — starts a COPY FROM STDIN operation
- `Session.send_copy_data(data)` — sends a chunk of data
- `Session.finish_copy()` — signals end of data
- `Session.abort_copy(reason)` — aborts the operation (server rolls back)

Usage:

```pony
actor BulkLoader is (SessionStatusNotify & ResultReceiver & CopyInReceiver)
  var _rows_sent: USize = 0

  be pg_session_authenticated(session: Session) =>
    session.copy_in(
      "COPY my_table (name, value) FROM STDIN", this)

  be pg_copy_ready(session: Session) =>
    _rows_sent = _rows_sent + 1
    if _rows_sent <= 3 then
      // Tab-delimited, newline-terminated rows
      let row: Array[U8] val = recover val
        ("row" + _rows_sent.string() + "\t" + (_rows_sent * 10).string()
          + "\n").array()
      end
      session.send_copy_data(row)
    else
      session.finish_copy()
    end

  be pg_copy_complete(session: Session, count: USize) =>
    // count = number of rows copied
    env.out.print("Copied " + count.string() + " rows")

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    // handle error
```

Data format depends on the COPY command — the default is tab-delimited text with newline row terminators. The pull-based design provides bounded memory usage: only one chunk is in flight at a time.

## Add bytea type conversion

PostgreSQL `bytea` columns are now automatically decoded from hex format into `Bytea`, a wrapper around `Array[U8] val`. Previously, bytea values were returned as raw hex strings (e.g., `\x48656c6c6f`). They are now decoded into `Bytea` values whose `.data` field contains the raw bytes.

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let bytes: Bytea =>
          // Decoded bytes — e.g., [72; 101; 108; 108; 111] for "Hello"
          for b in bytes.data.values() do
            _env.out.print("byte: " + b.string())
          end
        end
      end
    end
  end
```

Existing code is unaffected — if your `match` on `field.value` doesn't include a `Bytea` arm, bytea values simply won't match any branch (Pony's match is non-exhaustive).

## Add ParameterStatus tracking

PostgreSQL sends ParameterStatus messages during connection startup to report runtime parameter values (server_version, client_encoding, standard_conforming_strings, etc.) and again whenever a `SET` command changes a reporting parameter. Previously, the driver silently discarded these messages.

A new `pg_parameter_status` callback on `SessionStatusNotify` delivers each parameter as a `ParameterStatus` value with `name` and `value` fields:

```pony
actor MyNotify is SessionStatusNotify
  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    _env.out.print(status.name + " = " + status.value)
```

The callback has a default no-op implementation, so existing code is unaffected.

## Add COPY TO STDOUT support

You can now bulk-export data from PostgreSQL using `COPY ... TO STDOUT`. Call `session.copy_out()` with a COPY SQL statement and a `CopyOutReceiver` to start the operation. The server drives the flow — data arrives via `pg_copy_data` callbacks, and `pg_copy_complete` fires when all data has been delivered.

```pony
actor Exporter is (SessionStatusNotify & ResultReceiver & CopyOutReceiver)
  var _buffer: Array[U8] iso = recover iso Array[U8] end

  be pg_session_authenticated(session: Session) =>
    session.copy_out("COPY my_table TO STDOUT", this)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _buffer.append(data)

  be pg_copy_complete(session: Session, count: USize) =>
    let received = String.from_iso_array(
      _buffer = recover iso Array[U8] end)
    _env.out.print("Exported " + count.string() + " rows")
    _env.out.print(received)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    // handle error
```

Data format depends on the COPY command — the default is tab-delimited text with newline row terminators. Data chunks do not necessarily align with row boundaries; the receiver should buffer chunks if row-level processing is needed.

## Add SSLPreferred mode

`SSLPreferred` is a new SSL mode equivalent to PostgreSQL's `sslmode=prefer`. It attempts SSL negotiation when connecting and falls back to plaintext if the server refuses. A TLS handshake failure (server accepts but handshake fails) is a hard failure — the connection is not retried as plaintext.

Use `SSLPreferred` when you want encryption if available but don't want to fail when connecting to servers that don't support SSL:

```pony
use "ssl/net"

let sslctx = recover val
  SSLContext
    .> set_client_verify(false)
    .> set_server_verify(false)
end

let session = Session(
  ServerConnectInfo(auth, host, port, SSLPreferred(sslctx)),
  DatabaseConnectInfo(user, password, database),
  notify)
```

The existing `SSLRequired` mode is unchanged — it still aborts if the server refuses SSL.

## Add query pipelining

Query pipelining sends multiple queries to the server in a single TCP write and processes all responses in order, reducing round-trip latency from N round trips to 1. Each query has its own error isolation boundary — if one fails, subsequent queries continue executing.

A new `PipelineReceiver` interface provides three callbacks: `pg_pipeline_result` delivers individual query results with their pipeline index, `pg_pipeline_failed` delivers individual failures, and `pg_pipeline_complete` signals all queries have been processed.

```pony
// Pipeline 3 queries in a single call
let queries = recover val
  [as (PreparedQuery | NamedPreparedQuery):
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as FieldDataTypes: I32(1)] end)
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as FieldDataTypes: I32(2)] end)
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as FieldDataTypes: I32(3)] end)
  ]
end
session.pipeline(queries, my_receiver)

// In the receiver:
be pg_pipeline_result(session: Session, index: USize, result: Result) =>
  // Handle result for query at `index`

be pg_pipeline_complete(session: Session) =>
  // All queries processed
```

Only `PreparedQuery` and `NamedPreparedQuery` are supported — pipelining uses the extended query protocol.

## Change PreparedQuery and NamedPreparedQuery parameters to typed FieldDataTypes

`PreparedQuery` and `NamedPreparedQuery` parameters changed from `Array[(String | None)] val` to `Array[FieldDataTypes] val`. Typed values (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`, `PgTimestamp`, `PgTime`, `PgDate`, `PgInterval`) are now sent in binary wire format with explicit type OIDs, while `String` and `None` continue to use text format with server-inferred types.

Binary encoding eliminates the text→binary conversion the server previously had to do for every typed parameter. It also removes a class of silent bugs where a string like `"42"` could be interpreted as different types depending on context.

Before:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as (String | None): "42"] end)
```

After:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as FieldDataTypes: I32(42)] end)
```

String parameters still work — use them for text values or when you want the server to infer the type:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE name = $1",
  recover val [as FieldDataTypes: "Alice"] end)
```

If a parameter value can't be encoded (type mismatch with the codec), the query fails with `DataError` via `pg_query_failed` instead of being sent to the server.

## Change Result and ClientQueryError from traits to union types

`Result` and `ClientQueryError` are now union types instead of traits, enabling compiler-enforced exhaustive matching via `match \exhaustive\`. Previously, the compiler could not verify that all result or error variants were handled. Now, adding `\exhaustive\` to a match on `Result` or `ClientQueryError` produces a compile error if any variant is missing.

This matches the pattern already used by `AuthenticationFailureReason`, `TransactionStatus`, `Query`, `SSLMode`, and `FieldDataTypes` in this library.

Before:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let r: ResultSet => // ...
  | let r: RowModifying => // ...
  // SimpleResult silently unhandled — no compiler warning
  end
```

After:

```pony
be pg_query_result(session: Session, result: Result) =>
  match \exhaustive\ result
  | let r: ResultSet => // ...
  | let r: RowModifying => // ...
  | let r: SimpleResult => // ...
  end
```

Existing non-exhaustive matches continue to work without changes. The `query()` method remains callable on all three `Result` members without matching first.

## Change extended query results to binary format with typed temporal values

Extended query results (`PreparedQuery`, `NamedPreparedQuery`, streaming, pipelining) now use PostgreSQL's binary wire format instead of text. This means result values are decoded from their native binary representation rather than parsed from text strings. SimpleQuery results are unaffected — they continue using text format.

This change expands `FieldDataTypes` with four new temporal types and changes the decode type for some PostgreSQL OIDs:

- `date` columns now decode to `PgDate` (was `String`)
- `time` columns now decode to `PgTime` (was `String`)
- `timestamp` and `timestamptz` columns now decode to `PgTimestamp` (was `String`)
- `interval` columns now decode to `PgInterval` (was `String`)
- `oid`, `numeric`, `uuid`, and `jsonb` columns now decode to `String` with proper formatting (was `String` in text mode; now binary-decoded)
- Columns with unknown OIDs now decode to `RawBytes` (was `String` in text mode)

Any code with exhaustive `match` on `FieldDataTypes` or `field.value` must add arms for the new temporal types.

Before:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let s: String => _env.out.print(field.name + ": " + s)
        | let i: I32 => _env.out.print(field.name + ": " + i.string())
        | let b: Bool => _env.out.print(field.name + ": " + b.string())
        | let v: Bytea =>
          _env.out.print(field.name + ": bytes")
        | None => _env.out.print(field.name + ": NULL")
        end
      end
    end
  end
```

After:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let s: String => _env.out.print(field.name + ": " + s)
        | let i: I32 => _env.out.print(field.name + ": " + i.string())
        | let b: Bool => _env.out.print(field.name + ": " + b.string())
        | let v: Bytea =>
          _env.out.print(field.name + ": bytes")
        | let t: PgTimestamp => _env.out.print(field.name + ": " + t.string())
        | let t: PgDate => _env.out.print(field.name + ": " + t.string())
        | let t: PgTime => _env.out.print(field.name + ": " + t.string())
        | let t: PgInterval => _env.out.print(field.name + ": " + t.string())
        | None => _env.out.print(field.name + ": NULL")
        end
      end
    end
  end
```

The temporal types store their raw PostgreSQL values and provide `string()` methods that format them in standard PostgreSQL output format. `PgTimestamp` and `PgDate` support infinity via `I64.max_value()`/`I64.min_value()` and `I32.max_value()`/`I32.min_value()` respectively. `PgTime` uses constrained types — construct via `MakePgTimeMicroseconds` to validate the microseconds value, then pass the result to `PgTime.create()`.

## Add custom codec registry

You can now register custom codecs for PostgreSQL types not covered by the built-in codecs. Implement the `Codec` interface to decode a PostgreSQL type, define a result class implementing the `FieldData` interface, and register the codec with `CodecRegistry.with_codec()`:

```pony
// Custom result type for PostgreSQL point (OID 600)
class val Point is FieldData
  let x: F64
  let y: F64

  new val create(x': F64, y': F64) =>
    x = x'
    y = y'

  fun string(): String iso^ =>
    recover iso String .> append("(" + x.string() + "," + y.string() + ")") end

// Custom binary codec
primitive PointBinaryCodec is Codec
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    error

  fun decode(data: Array[U8] val): FieldData ? =>
    if data.size() != 16 then error end
    let x = ifdef bigendian then
      F64.from_bits(data.read_u64(0)?)
    else
      F64.from_bits(data.read_u64(0)?.bswap())
    end
    let y = ifdef bigendian then
      F64.from_bits(data.read_u64(8)?)
    else
      F64.from_bits(data.read_u64(8)?.bswap())
    end
    Point(x, y)

// Register and pass to Session
let registry = CodecRegistry
  .with_codec(600, PointBinaryCodec)?
let session = Session(server_info, db_info, notify where registry = registry)

// Match on the custom type in results
match field.value
| let p: Point => env.out.print(p.string())
end
```

Custom types that need to participate in `Field.eq()` comparisons should also implement `FieldDataEquatable`.

## Change result field values from closed union to open interface

`Field.value` is now `FieldData` (an open interface) instead of `FieldDataTypes` (a closed union). This is what enables custom codec result types — any `val` class with a `string()` method can be a field value.

As part of this change, `Array[U8] val` no longer appears directly as a result field value. Two new wrapper types replace it:

- `Bytea` — wraps `Array[U8] val` for known bytea columns (OID 17). Access the raw bytes via `.data`.
- `RawBytes` — wraps `Array[U8] val` for unknown binary-format OIDs. Access the raw bytes via `.data`.

Before:

```pony
match field.value
| let v: Array[U8] val =>
  for b in v.values() do
    _env.out.print("byte: " + b.string())
  end
end
```

After:

```pony
match field.value
| let v: Bytea =>
  for b in v.data.values() do
    _env.out.print("byte: " + b.string())
  end
| let v: RawBytes =>
  for b in v.data.values() do
    _env.out.print("byte: " + b.string())
  end
end
```

`Session.create()` now accepts an optional `registry` parameter (defaults to `CodecRegistry` with all built-in codecs):

```pony
// Default — same as before
let session = Session(server_info, db_info, notify)

// With custom codecs
let registry = CodecRegistry.with_codec(600, PointBinaryCodec)?
let session = Session(server_info, db_info, notify where registry = registry)
```

## Add 1-dimensional array type support

PostgreSQL array columns are now automatically decoded into `PgArray` values, and `PgArray` can be used as a query parameter. All built-in element types are supported: bool, bytea, int2, int4, int8, float4, float8, text, date, time, timestamp, timestamptz, interval, uuid, jsonb, numeric, and text-like types (char, name, xml, bpchar, varchar).

```pony
// Decoding from query results
match field.value
| let a: PgArray =>
  for elem in a.elements.values() do
    match elem
    | let v: I32 => // use v
    | None => // NULL element
    end
  end
end

// Encoding as a query parameter
let arr = PgArray(23,
  recover val [as (FieldData | None): I32(1); I32(2); None; I32(4)] end)
session.execute(PreparedQuery("SELECT $1::int4[]",
  recover val [as FieldDataTypes: arr] end), receiver)
```

`PgArray` works with both `SimpleQuery` (text format) and `PreparedQuery`/`NamedPreparedQuery` (binary format). Custom array types are supported via `CodecRegistry.with_array_type()`:

```pony
let registry = CodecRegistry
  .with_codec(600, PointBinaryCodec)?
  .with_array_type(1017, 600)?
```

Multi-dimensional arrays are not supported and will fall back to `String` (text format) or `RawBytes` (binary format).

## Change pg_session_connection_failed to include a failure reason

`pg_session_connection_failed` on `SessionStatusNotify` now takes a `ConnectionFailureReason` parameter indicating why the connection failed. This is a closed union type enabling exhaustive matching:

Before:

```pony
be pg_session_connection_failed(session: Session) =>
  _env.out.print("Connection failed")
```

After:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match reason
  | ConnectionFailedDNS => _env.out.print("DNS resolution failed")
  | ConnectionFailedTCP => _env.out.print("TCP connection failed")
  | SSLServerRefused => _env.out.print("Server refused SSL")
  | TLSAuthFailed => _env.out.print("TLS certificate error")
  | TLSHandshakeFailed => _env.out.print("TLS handshake failed")
  | ConnectionFailedTimeout => _env.out.print("Connection timed out")
  end
```

## Add ConnectionFailedTimeout to ConnectionFailureReason

`ConnectionFailureReason` now includes `ConnectionFailedTimeout` for when a connection attempt times out before a TCP or TLS connection is established. If you have an exhaustive match on `ConnectionFailureReason`, you'll need to add the new arm:

Before:

```pony
match reason
| ConnectionFailedDNS => _env.out.print("DNS resolution failed")
| ConnectionFailedTCP => _env.out.print("TCP connection failed")
| SSLServerRefused => _env.out.print("Server refused SSL")
| TLSAuthFailed => _env.out.print("TLS certificate error")
| TLSHandshakeFailed => _env.out.print("TLS handshake failed")
end
```

After:

```pony
match reason
| ConnectionFailedDNS => _env.out.print("DNS resolution failed")
| ConnectionFailedTCP => _env.out.print("TCP connection failed")
| SSLServerRefused => _env.out.print("Server refused SSL")
| TLSAuthFailed => _env.out.print("TLS certificate error")
| TLSHandshakeFailed => _env.out.print("TLS handshake failed")
| ConnectionFailedTimeout => _env.out.print("Connection timed out")
end
```

## Add statement timeout

All query operations (`execute`, `prepare`, `copy_in`, `copy_out`, `stream`, `pipeline`) now accept an optional `statement_timeout` parameter. When provided, the driver starts a one-shot timer and sends a CancelRequest if the operation does not complete within the given duration. The cancelled query fails with SQLSTATE 57014 (`query_canceled`), the same as a manual `cancel()` call.

```pony
match lori.MakeTimerDuration(5000) // 5 seconds
| let d: lori.TimerDuration =>
  session.execute(query, receiver where statement_timeout = d)
end
```

The timeout covers the entire operation: for streaming queries, from the initial Execute to the final ReadyForQuery; for pipelines, from the first query to the last. The timer is automatically cancelled when the operation completes normally.

## Add connection timeout

You can now set a timeout on the TCP connection phase by passing a `connection_timeout` to `ServerConnectInfo`. If the server is unreachable within the given duration, `pg_session_connection_failed` fires with `ConnectionFailedTimeout` instead of hanging indefinitely. Construct the timeout with `lori.MakeConnectionTimeout(milliseconds)`.

```pony
match lori.MakeConnectionTimeout(5000)
| let ct: lori.ConnectionTimeout =>
  let session = Session(
    ServerConnectInfo(auth, host, port
      where connection_timeout' = ct),
    DatabaseConnectInfo(username, password, database),
    notify)
end
```

Without a connection timeout (the default), connection attempts have no time bound and rely on the operating system's TCP timeout behavior.

## Add cleartext password authentication

Sessions can now authenticate to PostgreSQL servers that require cleartext password authentication. Previously, connecting to such a server would fire `pg_session_authentication_failed` with `UnsupportedAuthenticationMethod`.

No API changes are needed. The driver detects the server's requested authentication method and sends the password from `DatabaseConnectInfo` automatically, the same as it does for MD5 and SCRAM-SHA-256.

## Fix crash when closing a Session before connection initialization completes

Closing a `Session` immediately after creating it could crash if the close message arrived before the underlying connection actor finished its internal initialization. This was a race condition between Pony's causal messaging guarantees — the initialization message (self-to-self) and the close message (external sender) have no ordering guarantee. The race was unlikely but was observed on macOS arm64.

## Add enum type support

PostgreSQL enum columns return `RawBytes` when queried with `PreparedQuery` (binary format) because the driver doesn't recognize their dynamically-assigned OIDs. `SimpleQuery` (text format) already returns `String` via the unknown-OID fallback, but binary format had no equivalent.

`CodecRegistry.with_enum_type(oid)` registers an enum OID so both text and binary formats decode as `String`:

```pony
// Discover the OID once (e.g., SELECT oid FROM pg_type WHERE typname = 'mood')
let registry = CodecRegistry.with_enum_type(12345)?
let session = Session(server_info, db_info, notify where registry = registry)
```

Multiple enums and enum arrays compose naturally:

```pony
let registry = CodecRegistry
  .with_enum_type(12345)?          // mood
  .with_enum_type(12346)?          // color
  .with_array_type(12350, 12345)?  // mood[]
```

Enum values arrive as `String` in query results. No changes to `FieldDataTypes`, `Session`, or the `Codec` interface.

## Add composite type support

The driver now supports PostgreSQL composite types (user-defined structured types created with `CREATE TYPE ... AS (...)`). Composite values are decoded as `PgComposite` in query results and can be sent as query parameters.

Register composite types with `CodecRegistry.with_composite_type()`:

```pony
// CREATE TYPE address AS (street text, city text, zip_code int4)
// OID discovered via: SELECT oid FROM pg_type WHERE typname = 'address'

let registry = CodecRegistry
  .with_composite_type(16400,
    recover val
      [as (String, U32): ("street", 25); ("city", 25); ("zip_code", 23)]
    end)?
  .with_array_type(16401, 16400)?  // address[]
```

Access fields by position or name:

```pony
match field.value
| let addr: PgComposite =>
  match try addr(0)? end       // positional
  | let street: String => // ...
  end
  match try addr.field("city")? end  // named
  | let city: String => // ...
  end
end
```

Send composites as query parameters using `from_fields` for safe construction:

```pony
let addr = PgComposite.from_fields(16400,
  recover val
    [as (String, U32, (FieldData | None)):
      ("street", 25, "123 Main St")
      ("city", 25, "Springfield")
      ("zip_code", 23, I32(62704))]
  end)
session.execute(PreparedQuery("INSERT INTO users (home) VALUES ($1)",
  recover val [as FieldDataTypes: addr] end), receiver)
```

Nested composites and composite arrays are supported. Both `PreparedQuery` (binary format) and `SimpleQuery` (text format) decode composites.

