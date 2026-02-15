interface tag StreamingResultReceiver
  """
  Receives results from a `Session.stream()` call. Unlike `ResultReceiver`
  which buffers all rows before delivery, streaming delivers rows in
  fixed-size batches as they arrive from the server.

  The flow is pull-based: after each `pg_stream_batch`, call
  `session.fetch_more()` to request the next batch. When the server has
  no more rows, `pg_stream_complete` fires. Call `session.close_stream()`
  to end streaming early.
  """

  be pg_stream_batch(session: Session, rows: Rows)
    """
    Called when a batch of rows is available. The batch size is at most the
    `window_size` passed to `session.stream()`. After processing the batch,
    call `session.fetch_more()` to request the next batch.
    """

  be pg_stream_complete(session: Session)
    """
    Called when all rows have been delivered and the streaming query is
    finished. No further batches will arrive.
    """

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when the streaming query fails. The failure is either a server
    error (ErrorResponseMessage) or a client-side error (ClientQueryError)
    such as the session being closed or not yet authenticated.
    """
