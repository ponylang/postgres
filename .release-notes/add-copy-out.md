## Add COPY TO STDOUT support

You can now bulk-export data from PostgreSQL using `COPY ... TO STDOUT`. Call `session.copy_out()` with a COPY SQL statement and a `CopyOutReceiver` to start the operation. The server drives the flow — data arrives via `pg_copy_data` callbacks, and `pg_copy_complete` fires when all data has been delivered.

```pony
actor Exporter is (SessionStatusNotify & ResultReceiver & CopyOutReceiver)
  var _buffer: Array[U8] iso = recover iso Array[U8] end

  be pg_session_authenticated(session: Session) =>
    session.copy_out("COPY my_table TO STDOUT", this)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _buffer.append(data)

  be pg_copy_complete(session: Session, count: USize) =>
    let received = String.from_iso_array(
      _buffer = recover iso Array[U8] end)
    _env.out.print("Exported " + count.string() + " rows")
    _env.out.print(received)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    // handle error
```

Data format depends on the COPY command — the default is tab-delimited text with newline row terminators. Data chunks do not necessarily align with row boundaries; the receiver should buffer chunks if row-level processing is needed.
