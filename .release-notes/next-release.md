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

