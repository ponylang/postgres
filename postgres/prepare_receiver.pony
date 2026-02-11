interface tag PrepareReceiver
  """
  Receives the result of a `Session.prepare()` call. The session is passed to
  each callback so receivers can execute follow-up operations (such as
  executing the prepared statement) without needing to store a session
  reference.
  """
  be pg_statement_prepared(session: Session, name: String)
    """
    Called when the server has successfully prepared a named statement.
    """

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when statement preparation fails. The failure is either a server
    error (ErrorResponseMessage) or a client-side error (ClientQueryError)
    such as the session being closed or not yet authenticated.
    """
