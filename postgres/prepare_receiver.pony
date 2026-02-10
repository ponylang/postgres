interface tag PrepareReceiver
  """
  Receives the result of a `Session.prepare()` call.
  """
  be pg_statement_prepared(name: String)
    """
    Called when the server has successfully prepared a named statement.
    """

  be pg_prepare_failed(name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when statement preparation fails. The failure is either a server
    error (ErrorResponseMessage) or a client-side error (ClientQueryError)
    such as the session being closed or not yet authenticated.
    """
