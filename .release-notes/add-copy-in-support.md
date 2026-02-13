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
