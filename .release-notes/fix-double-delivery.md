## Fix double-delivery of pg_query_failed on failed transactions

When a query error occurred inside a PostgreSQL transaction, the `ResultReceiver` could receive `pg_query_failed` twice for the same query — once with the original error, and again with `SessionClosed` if `close()` was called before the session became idle. The errored query now correctly completes after `ReadyForQuery` regardless of transaction status.
