## Add row streaming support

Row streaming delivers query results in fixed-size batches instead of buffering all rows before delivery. This enables pull-based paged result consumption with bounded memory, ideal for large result sets.

A new `StreamingResultReceiver` interface provides three callbacks: `pg_stream_batch` delivers each batch of rows, `pg_stream_complete` signals all rows have been delivered, and `pg_stream_failed` reports errors. Three new `Session` methods control the flow:

```pony
// Start streaming with a window size of 100 rows per batch
session.stream(
  PreparedQuery("SELECT * FROM big_table",
    recover val Array[(String | None)] end),
  100, my_receiver)

// In the receiver:
be pg_stream_batch(session: Session, rows: Rows) =>
  // Process this batch
  session.fetch_more()  // Pull the next batch

be pg_stream_complete(session: Session) =>
  // All rows delivered
```

Call `session.close_stream()` to end streaming early. Only `PreparedQuery` and `NamedPreparedQuery` are supported — streaming uses the extended query protocol's `Execute(max_rows)` + `PortalSuspended` mechanism.
