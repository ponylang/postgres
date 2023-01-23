primitive SesssionNeverOpened is QueryError
  """
  Error returned when a query is attempted for a session that hasn't been opened
  yet or is in the process of being opened.
  """

primitive SessionClosed is QueryError
  """
  Error returned when a query is attempted for a session that was closed or
  failed to open. Includes sessions that were closed by the user as well as
  those closed due to connection failures, authentication failures, and
  connections that have been shut down due to unrecoverable Postgres protocol
  errors.
  """

primitive SessionNotAuthenticated is QueryError
  """
  Error returned when a query is attempted for a session that is open but hasn't
  been authenticated yet.
  """

