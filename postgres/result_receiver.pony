interface tag ResultReceiver
  """
  Receives the result of a `Session.execute()` call. The session is passed to
  each callback so receivers can execute follow-up queries without needing to
  store a session reference.
  """
  be pg_query_result(session: Session, result: Result)
    """
    Called when a query completes successfully.
    """

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when a query fails. The failure is either a server error
    (ErrorResponseMessage) or a client-side error (ClientQueryError) such as
    the session being closed or never opened.
    """
