# Examples

Each subdirectory is a self-contained Pony program demonstrating a different part of the postgres library.

## bytea

Binary data using `bytea` columns. Executes a SELECT that returns a bytea value, matches on `Array[U8] val` in the result, and prints the decoded bytes. Shows how the driver automatically decodes PostgreSQL's hex-format bytea representation into raw byte arrays.

## query

Minimal example using `SimpleQuery`. Connects, authenticates, executes `SELECT 525600::text`, and prints the result by iterating rows and matching on `FieldDataTypes`. Start here if you're new to the library.

## prepared-query

Parameterized queries using `PreparedQuery`. Sends a query with typed parameters (`text`, `int4`) and a NULL parameter, then inspects the `ResultSet`. Shows how to construct the `Array[(String | None)] val` parameter array.

## named-prepared-query

Named prepared statements using `Session.prepare()` and `NamedPreparedQuery`. Prepares a statement once, executes it twice with different parameters, then cleans up with `Session.close_statement()`. Shows how to implement `PrepareReceiver` for prepare lifecycle callbacks.

## ssl-query

SSL-encrypted query using `SSLRequired`. Same workflow as `query` but with TLS negotiation enabled. Demonstrates how to create an `SSLContext`, wrap it in `SSLRequired`, and pass it to `Session`. Requires a PostgreSQL server configured to accept SSL connections.

## cancel

Query cancellation using `Session.cancel()`. Executes a long-running query (`SELECT pg_sleep(10)`), cancels it, and handles the resulting `ErrorResponseMessage` with SQLSTATE `57014` (query_canceled) in the `ResultReceiver`. Shows that cancellation is best-effort and arrives as a query failure, not a separate callback.

## crud

Multi-query workflow mixing `SimpleQuery` and `PreparedQuery`. Creates a table, inserts rows with parameterized INSERTs, selects them back, deletes, and drops the table. Demonstrates all three `Result` types (`ResultSet`, `RowModifying`, `SimpleResult`) and `ErrorResponseMessage` error handling.

## listen-notify

Asynchronous notifications using PostgreSQL's LISTEN/NOTIFY mechanism. Subscribes to a channel with `LISTEN`, sends a notification with `NOTIFY`, receives it via the `pg_notification` callback on `SessionStatusNotify`, and unsubscribes with `UNLISTEN`. Shows the `Notification` class fields (channel, payload, pid).

## copy-in

Bulk data loading using `COPY ... FROM STDIN`. Creates a table, loads three rows of tab-delimited text data via `Session.copy_in()`, verifies the data with a SELECT, then drops the table. Demonstrates the pull-based `CopyInReceiver` interface: `pg_copy_ready` fires after each `send_copy_data`, and `finish_copy` completes the operation.

## notice

Server notice handling using `pg_notice`. Executes `DROP TABLE IF EXISTS` on a nonexistent table, which triggers a PostgreSQL `NoticeResponse`, and prints the notice fields (severity, code, message). Shows how `SessionStatusNotify.pg_notice` delivers non-fatal informational messages from the server.

## transaction-status

Transaction status tracking using `pg_transaction_status`. Sends `BEGIN` and `COMMIT` and prints the `TransactionStatus` reported at each step. Shows how `SessionStatusNotify.pg_transaction_status` fires on every `ReadyForQuery` with `TransactionIdle`, `TransactionInBlock`, or `TransactionFailed`.
