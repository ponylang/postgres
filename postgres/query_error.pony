trait val ClientQueryError
  """
  A client-side error that prevented a query from being sent to the server.
  Each subtype represents a specific pre-condition failure (session not open,
  session closed, not authenticated, or malformed data).
  """

primitive SessionNeverOpened is ClientQueryError
  """
  Error returned when a query is attempted for a session that hasn't been opened
  yet or is in the process of being opened.
  """

primitive SessionClosed is ClientQueryError
  """
  Error returned when a query is attempted for a session that was closed or
  failed to open. Includes sessions that were closed by the user as well as
  those closed due to connection failures, authentication failures, and
  connections that have been shut down due to unrecoverable Postgres protocol
  errors.
  """

primitive SessionNotAuthenticated is ClientQueryError
  """
  Error returned when a query is attempted for a session that is open but hasn't
  been authenticated yet.
  """

primitive DataError is ClientQueryError
  """
  Error returned when the data that came back from a query is in a format that
  this library doesn't expect. This might indicate something like, the number
  of columns across rows returned doesn't match or other "this should never
  happen" type of errors.
  """
