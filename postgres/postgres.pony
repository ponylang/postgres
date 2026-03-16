"""
# Pony PostgreSQL Driver

Pure Pony driver for PostgreSQL. Alpha-level — the API may change
between releases.

## Connecting

Create a `Session` with server and database connection info plus a
`SessionStatusNotify` to receive lifecycle events:

```pony
use "postgres"
use lori = "lori"

actor Main
  new create(env: Env) =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(env.root), "localhost", "5432"),
      DatabaseConnectInfo("myuser", "mypassword", "mydb"),
      MyNotify(env))

actor MyNotify is (SessionStatusNotify & ResultReceiver)
  let _env: Env

  new create(env: Env) =>
    _env = env

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    _env.out.print("Got a result!")
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _env.out.print("Query failed")
    session.close()
```

`SessionStatusNotify` callbacks are all optional (default no-op).
Implement the ones you need:

* `pg_session_connected` / `pg_session_connection_failed` — TCP
  connection established or failed
* `pg_session_authenticated` / `pg_session_authentication_failed` —
  login succeeded or failed
* `pg_transaction_status` — fires on every `ReadyForQuery` with
  `TransactionIdle`, `TransactionInBlock`, or `TransactionFailed`
* `pg_notification` — LISTEN/NOTIFY notifications
* `pg_notice` — non-fatal server notices (e.g., `DROP IF EXISTS` on
  a nonexistent table, `RAISE NOTICE` from PL/pgSQL)
* `pg_parameter_status` — runtime parameter values (sent during startup
  and after SET commands)
* `pg_session_shutdown` — session has shut down

## SSL/TLS

Two SSL modes are available:

* **`SSLRequired`** — aborts if the server refuses SSL. Use when
  encryption is mandatory.
* **`SSLPreferred`** — attempts SSL, falls back to plaintext if the
  server refuses. Equivalent to PostgreSQL's `sslmode=prefer`. A TLS
  handshake failure (server accepts but handshake fails) is NOT retried
  as plaintext — `pg_session_connection_failed` fires.

```pony
use "ssl/net"

let sslctx = recover val
  SSLContext
    .> set_client_verify(true)
    .> set_authority(FilePath(FileAuth(env.root), "/path/to/ca.pem"))?
end

// Require SSL — fail if server refuses
let session = Session(
  ServerConnectInfo(
    lori.TCPConnectAuth(env.root), "localhost", "5432",
    SSLRequired(sslctx)),
  DatabaseConnectInfo("myuser", "mypassword", "mydb"),
  MyNotify(env))

// Prefer SSL — fall back to plaintext if server refuses
let session2 = Session(
  ServerConnectInfo(
    lori.TCPConnectAuth(env.root), "localhost", "5432",
    SSLPreferred(sslctx)),
  DatabaseConnectInfo("myuser", "mypassword", "mydb"),
  MyNotify(env))
```

## Executing Queries

Three query types are available, all executed via `session.execute()`:

* **`SimpleQuery`** — an unparameterized SQL string. Can contain
  multiple semicolon-separated statements; each produces a separate
  result callback.

* **`PreparedQuery`** — a parameterized single statement using `$1`,
  `$2`, etc. Parameters are `Array[FieldDataTypes] val` — typed values
  (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`,
  `PgArray`, `PgTimestamp`, `PgTime`, `PgDate`, `PgInterval`) are sent
  in binary format with explicit OIDs, while `String` and `None` use
  text format with server-inferred types. Uses an unnamed server-side
  prepared statement (created and destroyed per execution).

* **`NamedPreparedQuery`** — executes a previously prepared named
  statement (see `session.prepare()`). Same typed parameter semantics
  as `PreparedQuery`. Use this when executing the same parameterized
  query many times to avoid repeated parsing.

For one-off parameterized queries, prefer `PreparedQuery`. Use
`NamedPreparedQuery` only when you need to reuse a prepared statement
across multiple executions.

Results arrive via `ResultReceiver`:

* `pg_query_result` delivers a `Result`, which is one of:
  - `ResultSet` — rows returned (SELECT, RETURNING, etc.)
  - `RowModifying` — row count returned (INSERT, UPDATE, DELETE)
  - `SimpleResult` — no data (empty query)

* `pg_query_failed` delivers an `ErrorResponseMessage` (server error)
  or a `ClientQueryError` (`SessionClosed`, `SessionNeverOpened`,
  `SessionNotAuthenticated`, `DataError`).

## Working with Results

`ResultSet` contains typed `Rows`:

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
          _env.out.print(field.name + ": " + v.data.size().string() + " bytes")
        | let a: PgArray =>
          _env.out.print(field.name + ": " + a.string())
        | let t: PgTimestamp => _env.out.print(field.name + ": " + t.string())
        | let t: PgDate => _env.out.print(field.name + ": " + t.string())
        | let t: PgTime => _env.out.print(field.name + ": " + t.string())
        | let t: PgInterval => _env.out.print(field.name + ": " + t.string())
        | None => _env.out.print(field.name + ": NULL")
        // Also: I16, I64, F32, F64
        end
      end
    end
  | let rm: RowModifying =>
    _env.out.print(rm.command() + ": " + rm.impacted().string() + " rows")
  end
```

Field values are typed based on the PostgreSQL column OID:
bytea → `Bytea`, bool → `Bool`, int2 → `I16`, int4 → `I32`,
int8 → `I64`, float4 → `F32`, float8 → `F64`, date → `PgDate`,
time → `PgTime`, timestamp/timestamptz → `PgTimestamp`,
interval → `PgInterval`, array types → `PgArray`, NULL → `None`.
Extended query results use binary format — unknown OIDs produce
`RawBytes`. Simple query results use text format — unknown OIDs
produce `String`.

**`timestamptz` and query format:** `PreparedQuery` results use binary
format where `timestamptz` values are always UTC microseconds.
`SimpleQuery` results use text format where the server renders the
value in the session's timezone and the driver strips the timezone
suffix — the resulting `PgTimestamp` microseconds represent
session-local time, not UTC. If your session timezone is not UTC and
you use both query types on the same `timestamptz` column, the
microsecond values will differ for the same row.

## Named Prepared Statements

Prepare once, execute many times:

```pony
be pg_session_authenticated(session: Session) =>
  session.prepare("find_user", "SELECT * FROM users WHERE id = $1", this)

be pg_statement_prepared(session: Session, name: String) =>
  session.execute(
    NamedPreparedQuery(name,
      recover val [as FieldDataTypes: I32(42)] end),
    this)
  session.execute(
    NamedPreparedQuery(name,
      recover val [as FieldDataTypes: I32(99)] end),
    this)
  // When done, optionally: session.close_statement(name)
```

This requires implementing `PrepareReceiver` alongside
`ResultReceiver`.

## Bulk Loading with COPY IN

`session.copy_in()` sends data to the server via the COPY FROM STDIN
protocol. It uses a pull-based flow — the session calls
`pg_copy_ready` on the `CopyInReceiver` to request each chunk:

```pony
be pg_session_authenticated(session: Session) =>
  session.copy_in(
    "COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)", this)

be pg_copy_ready(session: Session) =>
  if _has_more_data then
    session.send_copy_data("val1\tval2\n".array())
  else
    session.finish_copy()
  end

be pg_copy_complete(session: Session, count: USize) =>
  _env.out.print("Loaded " + count.string() + " rows")
```

Call `session.abort_copy(reason)` instead of `finish_copy()` to
abort the operation.

## Bulk Export with COPY OUT

`session.copy_out()` exports data from the server via the COPY TO STDOUT
protocol. The server drives the flow — data arrives via `pg_copy_data`
callbacks on the `CopyOutReceiver`:

```pony
be pg_session_authenticated(session: Session) =>
  session.copy_out("COPY my_table TO STDOUT", this)

be pg_copy_data(session: Session, data: Array[U8] val) =>
  _buffer.append(data)

be pg_copy_complete(session: Session, count: USize) =>
  _env.out.print("Exported " + count.string() + " rows")
```

## Row Streaming

`session.stream()` delivers rows in windowed batches using the
extended query protocol's portal suspension mechanism. Unlike
`execute()` which buffers all rows before delivery, streaming enables
pull-based paged result consumption with bounded memory:

```pony
be pg_session_authenticated(session: Session) =>
  session.stream(
    PreparedQuery("SELECT * FROM big_table",
      recover val Array[FieldDataTypes] end),
    100, this)  // window_size = 100 rows per batch

be pg_stream_batch(session: Session, rows: Rows) =>
  // Process this batch of up to 100 rows
  for row in rows.values() do
    // ...
  end
  session.fetch_more()  // Pull the next batch

be pg_stream_complete(session: Session) =>
  _env.out.print("All rows processed")
```

Call `session.close_stream()` to end streaming early. Only
`PreparedQuery` and `NamedPreparedQuery` are supported — streaming
requires the extended query protocol.

## Query Pipelining

`session.pipeline()` sends multiple queries to the server in a single TCP
write, reducing round-trip latency from N round trips to 1. Each query has
its own `Sync` boundary for error isolation — if one query fails, subsequent
queries continue executing.

Only `PreparedQuery` and `NamedPreparedQuery` are supported. Results arrive
via `PipelineReceiver`:

* `pg_pipeline_result` — individual query succeeded, with its pipeline index
* `pg_pipeline_failed` — individual query failed, with its pipeline index
* `pg_pipeline_complete` — all queries processed (always fires last)

```pony
be pg_session_authenticated(session: Session) =>
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
  session.pipeline(queries, this)

be pg_pipeline_result(session: Session, index: USize, result: Result) =>
  _env.out.print("Query " + index.string() + " succeeded")

be pg_pipeline_failed(session: Session, index: USize,
  query: (PreparedQuery | NamedPreparedQuery),
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  _env.out.print("Query " + index.string() + " failed")

be pg_pipeline_complete(session: Session) =>
  _env.out.print("All pipeline queries processed")
```

## Query Cancellation

`session.cancel()` requests cancellation of the currently executing
query. Cancellation is best-effort — the server may or may not honor
it. If cancelled, the query's `ResultReceiver` receives
`pg_query_failed` with SQLSTATE 57014. Queued queries are not
affected.

## Array Types

1-dimensional PostgreSQL arrays are automatically decoded into `PgArray`
values. All built-in element types are supported (int2, int4, int8,
float4, float8, bool, text, bytea, date, time, timestamp, timestamptz,
interval, uuid, jsonb, numeric, and text-like types).

`PgArray` can also be used as a query parameter:

```pony
let arr = PgArray(23,
  recover val [as (FieldData | None): I32(1); I32(2); None; I32(4)] end)
session.execute(PreparedQuery("SELECT $1::int4[]",
  recover val [as FieldDataTypes: arr] end), receiver)
```

For custom array types (arrays of custom codec-registered OIDs), use
`CodecRegistry.with_array_type()`:

```pony
let registry = CodecRegistry
  .with_codec(600, PointBinaryCodec)?
  .with_array_type(1017, 600)?
```

## Custom Codecs

Extend the driver with custom type decoders. Implement `Codec` to decode
a PostgreSQL type, then register it with `CodecRegistry.with_codec()`:

```pony
class val Point is FieldData
  let x: F64
  let y: F64

  new val create(x': F64, y': F64) =>
    x = x'
    y = y'

  fun string(): String iso^ =>
    recover iso String .> append("(" + x.string() + "," + y.string() + ")") end

primitive PointBinaryCodec is Codec
  fun format(): U16 => 1  // binary

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    error  // encode not needed for result-only types

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
let registry = CodecRegistry.with_codec(600, PointBinaryCodec)?
let session = Session(server_info, db_info, notify where registry = registry)
```

Result fields from decoded custom types can be matched directly:

```pony
match field.value
| let p: Point => _env.out.print(p.string())
end
```

Custom types that need to participate in `Field.eq()` comparisons should
also implement `FieldDataEquatable`.

## Authentication

The driver supports MD5 password and SCRAM-SHA-256 authentication.
SCRAM-SHA-256 is the PostgreSQL default since version 10. Cleartext
password, Kerberos, GSS, and certificate authentication are not
supported.

## Supported Features

* Simple and extended query protocols
* Typed parameterized queries with binary encoding for numeric, boolean,
  bytea, and temporal types (unnamed and named prepared statements)
* SSL/TLS via `SSLRequired` and `SSLPreferred`
* MD5 and SCRAM-SHA-256 authentication
* Transaction status tracking (`TransactionStatus`)
* LISTEN/NOTIFY notifications
* NoticeResponse delivery (non-fatal server messages)
* COPY FROM STDIN (bulk data loading)
* COPY TO STDOUT (bulk data export)
* Row streaming (windowed batch delivery)
* Query pipelining (batched multi-query execution)
* Query cancellation
* ParameterStatus tracking (server runtime parameters)
* 1-dimensional array types (decode and encode via `PgArray`)
* Custom codecs via `CodecRegistry.with_codec()`
* Custom array types via `CodecRegistry.with_array_type()`
"""
