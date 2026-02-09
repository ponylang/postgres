## Fix double-delivery of pg_query_failed when close() races with error processing

When `close()` was called while the session was between processing an error response and processing the subsequent ready-for-query message, the `ResultReceiver` could receive `pg_query_failed` twice — once with the original error and again with `SessionClosed`. Query cycle messages are now processed synchronously, preventing other operations from interleaving.
