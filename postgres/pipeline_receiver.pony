interface tag PipelineReceiver
  """
  Receives results from a `Session.pipeline()` call. Each query in the
  pipeline is processed independently — if one fails, subsequent queries
  continue executing. Results are delivered in pipeline order via indexed
  callbacks.

  `pg_pipeline_complete` always fires last, even during shutdown, giving
  the receiver a terminal callback regardless of individual query outcomes.
  """

  be pg_pipeline_result(session: Session, index: USize, result: Result)
    """
    Called when an individual query in the pipeline succeeds. The `index`
    corresponds to the query's position in the array passed to
    `session.pipeline()`.
    """

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when an individual query in the pipeline fails. The `index`
    corresponds to the query's position in the array passed to
    `session.pipeline()`. Remaining queries in the pipeline continue
    executing — each Sync creates an error isolation boundary. The
    failure is either a server error (ErrorResponseMessage) or a
    client-side error (ClientQueryError), which includes
    `ProtocolViolation` when the server tore the session down with an
    invalid wire message; the currently-executing query sees
    `ProtocolViolation` and subsequent queries see `SessionClosed`.
    """

  be pg_pipeline_complete(session: Session)
    """
    Called when the entire pipeline has finished. All individual results
    and failures have been delivered before this callback fires. Always
    called exactly once per `session.pipeline()` invocation, even during
    shutdown.
    """
