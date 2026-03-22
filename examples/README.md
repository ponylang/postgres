# Examples

Each subdirectory is a self-contained Pony program demonstrating a different part of the postgres library.

## array

Array types using `PreparedQuery` with binary-format results and `PgArray` parameters. Executes a SELECT that returns an `int4[]` literal, matches on `PgArray` in the result and iterates the decoded elements, then sends a `PgArray` parameter containing `NULL` elements back as a query parameter to verify the encode/decode roundtrip.

## bytea

Binary data using `bytea` columns. Executes a SELECT that returns a bytea value, matches on `Bytea` in the result, and prints the decoded bytes. Shows how the driver automatically decodes PostgreSQL's hex-format bytea representation into a `Bytea` wrapper around raw byte arrays.

## custom-codec

Custom type decoding via `CodecRegistry.with_codec()`. Defines a `Point` class implementing `FieldData` and a `PointBinaryCodec` for PostgreSQL's `point` type (OID 600), registers the codec, passes the extended `CodecRegistry` to `Session`, and matches on `Point` in query results. Shows how to extend the driver with decoders for PostgreSQL types not covered by the built-in codecs.

## query

Minimal example using `SimpleQuery`. Connects, authenticates, executes `SELECT 525600::text`, and prints the result by iterating rows and matching on field value types. Start here if you're new to the library.

## prepared-query

Parameterized queries using `PreparedQuery`. Sends a query with typed parameters (`text`, `int4`) and a NULL parameter, then inspects the `ResultSet`. Shows how to construct the `Array[FieldDataTypes] val` parameter array with typed values and NULL.

## named-prepared-query

Named prepared statements using `Session.prepare()` and `NamedPreparedQuery`. Prepares a statement once, executes it twice with different parameters, then cleans up with `Session.close_statement()`. Shows how to implement `PrepareReceiver` for prepare lifecycle callbacks.

## ssl-preferred-query

SSL-preferred query using `SSLPreferred`. Same workflow as `query` but with SSL negotiation that falls back to plaintext if the server refuses — equivalent to PostgreSQL's `sslmode=prefer`. Demonstrates the difference between `SSLPreferred` (best-effort encryption) and `SSLRequired` (mandatory encryption). Works with both SSL-enabled and non-SSL PostgreSQL servers.

## ssl-query

SSL-encrypted query using `SSLRequired`. Same workflow as `query` but with TLS negotiation enabled. Demonstrates how to create an `SSLContext`, wrap it in `SSLRequired`, and pass it to `Session`. Requires a PostgreSQL server configured to accept SSL connections.

## cancel

Query cancellation using `Session.cancel()`. Executes a long-running query (`SELECT pg_sleep(10)`), cancels it, and handles the resulting `ErrorResponseMessage` with SQLSTATE `57014` (query_canceled) in the `ResultReceiver`. Shows that cancellation is best-effort and arrives as a query failure, not a separate callback.

## connection-timeout

Connection timeout using the `connection_timeout` parameter on `ServerConnectInfo`. Connects to a configurable host and port with a 3-second timeout via `lori.MakeConnectionTimeout(3000)`, and handles `ConnectionFailedTimeout` in `pg_session_connection_failed`. Shows how the driver reports unreachable servers without hanging indefinitely.

## crud

Multi-query workflow mixing `SimpleQuery` and `PreparedQuery`. Creates a table, inserts rows with parameterized INSERTs, selects them back, deletes, and drops the table. Demonstrates all three `Result` types (`ResultSet`, `RowModifying`, `SimpleResult`) and `ErrorResponseMessage` error handling.

## listen-notify

Asynchronous notifications using PostgreSQL's LISTEN/NOTIFY mechanism. Subscribes to a channel with `LISTEN`, sends a notification with `NOTIFY`, receives it via the `pg_notification` callback on `SessionStatusNotify`, and unsubscribes with `UNLISTEN`. Shows the `Notification` class fields (channel, payload, pid).

## copy-in

Bulk data loading using `COPY ... FROM STDIN`. Creates a table, loads three rows of tab-delimited text data via `Session.copy_in()`, verifies the data with a SELECT, then drops the table. Demonstrates the pull-based `CopyInReceiver` interface: `pg_copy_ready` fires after each `send_copy_data`, and `finish_copy` completes the operation.

## copy-out

Bulk data export using `COPY ... TO STDOUT`. Creates a table, inserts three rows, exports them via `Session.copy_out()`, and prints the received data. Demonstrates the push-based `CopyOutReceiver` interface: the server drives the flow, calling `pg_copy_data` for each chunk, then `pg_copy_complete` when finished.

## streaming

Row streaming using `Session.stream()` with windowed batch delivery. Creates a table with 7 rows, streams them with `window_size=3` (producing batches of 3, 3, and 1), then drops the table. Demonstrates the pull-based `StreamingResultReceiver` interface: `pg_stream_batch` delivers each batch, `fetch_more()` requests the next, and `pg_stream_complete` signals completion.

## pipeline

Query pipelining using `Session.pipeline()` with reduced round-trip latency. Creates a table with 3 rows, pipelines 3 SELECTs with different WHERE clauses in a single call, and prints indexed results. Demonstrates the `PipelineReceiver` interface: `pg_pipeline_result` delivers individual query results with their index, `pg_pipeline_failed` delivers individual failures, and `pg_pipeline_complete` signals that all queries have been processed.

## notice

Server notice handling using `pg_notice`. Executes `DROP TABLE IF EXISTS` on a nonexistent table, which triggers a PostgreSQL `NoticeResponse`, and prints the notice fields (severity, code, message). Shows how `SessionStatusNotify.pg_notice` delivers non-fatal informational messages from the server.

## statement-timeout

Statement timeout using the `statement_timeout` parameter on `session.execute()`. Executes a long-running query (`SELECT pg_sleep(10)`) with a 2-second timeout via `lori.MakeTimerDuration(2000)`, and handles the resulting `ErrorResponseMessage` with SQLSTATE `57014` (query_canceled). Shows how the driver automatically cancels a query that exceeds the timeout, using the same CancelRequest mechanism as `session.cancel()`.

## temporal

Temporal types using `PreparedQuery` with binary-format results. Executes a SELECT that returns `date`, `time`, `timestamp`, and `interval` literals, then matches each field against its corresponding Pony type (`PgDate`, `PgTime`, `PgTimestamp`, `PgInterval`) and prints the `string()` representation. Shows how the driver automatically decodes PostgreSQL temporal wire formats into typed values.

## transaction-status

Transaction status tracking using `pg_transaction_status`. Sends `BEGIN` and `COMMIT` and prints the `TransactionStatus` reported at each step. Shows how `SessionStatusNotify.pg_transaction_status` fires on every `ReadyForQuery` with `TransactionIdle`, `TransactionInBlock`, or `TransactionFailed`.
