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

You can now encrypt connections to PostgreSQL using SSL/TLS. Pass `SSLRequired(sslctx)` to `Session.create()` to enable SSL negotiation before authentication. The default `SSLDisabled` preserves the existing plaintext behavior.

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
  auth,
  notify,
  host,
  port,
  username,
  password,
  database,
  SSLRequired(sslctx))
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

