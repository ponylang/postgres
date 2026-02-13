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
* `pg_session_shutdown` — session has shut down

## SSL/TLS

Pass `SSLRequired` to enable SSL negotiation:

```pony
use "ssl/net"

let sslctx = recover val
  SSLContext
    .> set_client_verify(true)
    .> set_authority(FilePath(FileAuth(env.root), "/path/to/ca.pem"))?
end

let session = Session(
  ServerConnectInfo(
    lori.TCPConnectAuth(env.root), "localhost", "5432",
    SSLRequired(sslctx)),
  DatabaseConnectInfo("myuser", "mypassword", "mydb"),
  MyNotify(env))
```

If the server refuses SSL, `pg_session_connection_failed` fires.

## Executing Queries

Three query types are available, all executed via `session.execute()`:

* **`SimpleQuery`** — an unparameterized SQL string. Can contain
  multiple semicolon-separated statements; each produces a separate
  result callback.

* **`PreparedQuery`** — a parameterized single statement using `$1`,
  `$2`, etc. Parameters are text-format strings or `None` for NULL.
  Uses an unnamed server-side prepared statement (created and destroyed
  per execution).

* **`NamedPreparedQuery`** — executes a previously prepared named
  statement (see `session.prepare()`). Use this when executing the
  same parameterized query many times to avoid repeated parsing.

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
bool → `Bool`, int2 → `I16`, int4 → `I32`, int8 → `I64`,
float4 → `F32`, float8 → `F64`, NULL → `None`, everything
else → `String`.

## Named Prepared Statements

Prepare once, execute many times:

```pony
be pg_session_authenticated(session: Session) =>
  session.prepare("find_user", "SELECT * FROM users WHERE id = $1", this)

be pg_statement_prepared(session: Session, name: String) =>
  session.execute(NamedPreparedQuery(name, ["42"]), this)
  session.execute(NamedPreparedQuery(name, ["99"]), this)
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

## Query Cancellation

`session.cancel()` requests cancellation of the currently executing
query. Cancellation is best-effort — the server may or may not honor
it. If cancelled, the query's `ResultReceiver` receives
`pg_query_failed` with SQLSTATE 57014. Queued queries are not
affected.

## Authentication

The driver supports MD5 password and SCRAM-SHA-256 authentication.
SCRAM-SHA-256 is the PostgreSQL default since version 10. Cleartext
password, Kerberos, GSS, and certificate authentication are not
supported.

## Supported Features

* Simple and extended query protocols
* Parameterized queries (unnamed and named prepared statements)
* SSL/TLS via `SSLRequired`
* MD5 and SCRAM-SHA-256 authentication
* Transaction status tracking (`TransactionStatus`)
* LISTEN/NOTIFY notifications
* COPY FROM STDIN (bulk data loading)
* Query cancellation
"""
