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
be pg_query_failed(query: Query,
  failure: (ErrorResponseMessage | ClientQueryError))
=>
  match query
  | let sq: SimpleQuery => // ...
  | let pq: PreparedQuery => // ...
  | let nq: NamedPreparedQuery => // ...
  end
```

