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

You can now execute parameterized queries using `PreparedQuery`. Parameters are referenced as `$1`, `$2`, etc. in the query string and passed as an array of `(String | None)` values. Use `None` for SQL NULL.

```pony
// Parameterized SELECT
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as (String | None): "42"] end)
session.execute(query, receiver)

// INSERT with NULL parameter
let insert = PreparedQuery("INSERT INTO items (name, desc) VALUES ($1, $2)",
  recover val [as (String | None): "widget"; None] end)
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
      recover val [as (String | None): "42"] end),
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

When a PostgreSQL server requested an authentication method the driver doesn't support (e.g., cleartext password, Kerberos, GSSAPI), the session would hang indefinitely with no error reported. It now correctly fails with `UnsupportedAuthenticationMethod` via the `pg_session_authentication_failed` callback.

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

PostgreSQL `bytea` columns are now automatically decoded from hex format into `Array[U8] val`. Previously, bytea values were returned as raw hex strings (e.g., `\x48656c6c6f`). They are now decoded into byte arrays that you can work with directly.

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let bytes: Array[U8] val =>
          // Decoded bytes — e.g., [72; 101; 108; 108; 111] for "Hello"
          for b in bytes.values() do
            _env.out.print("byte: " + b.string())
          end
        end
      end
    end
  end
```

Existing code is unaffected — if your `match` on `field.value` doesn't include an `Array[U8] val` arm, bytea values simply won't match any branch (Pony's match is non-exhaustive).

## Add ParameterStatus tracking

PostgreSQL sends ParameterStatus messages during connection startup to report runtime parameter values (server_version, client_encoding, standard_conforming_strings, etc.) and again whenever a `SET` command changes a reporting parameter. Previously, the driver silently discarded these messages.

A new `pg_parameter_status` callback on `SessionStatusNotify` delivers each parameter as a `ParameterStatus` value with `name` and `value` fields:

```pony
actor MyNotify is SessionStatusNotify
  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    _env.out.print(status.name + " = " + status.value)
```

The callback has a default no-op implementation, so existing code is unaffected.

