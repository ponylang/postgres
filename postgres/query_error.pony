// A non-server-error failure for a query. Most members represent client-side
// conditions that prevented the query from being sent or its result from
// being delivered (session not open, session closed, not authenticated,
// malformed data). `ProtocolViolation` is the exception: it is a
// server-caused failure detected during query handling — the server sent
// data that couldn't be parsed or that was invalid in the current state,
// and the session was torn down. The query may or may not have been
// processed on the server before the violation; treat the result as
// indeterminate.
type ClientQueryError is
  ( SessionNeverOpened
  | SessionClosed
  | SessionNotAuthenticated
  | DataError
  | ProtocolViolation )

primitive SessionNeverOpened
  """
  Error returned when a query is attempted for a session that hasn't been opened
  yet or is in the process of being opened.
  """

primitive SessionClosed
  """
  Error returned when a query is attempted for a session that was closed or
  failed to open. Includes sessions that were closed by the user as well as
  those closed due to connection failures, authentication failures, and
  connections that were shut down after a server protocol violation.
  Queries that were queued at the time of the protocol violation (not in
  flight) receive this error; the in-flight query, if any, receives
  `ProtocolViolation`.
  """

primitive SessionNotAuthenticated
  """
  Error returned when a query is attempted for a session that is open but hasn't
  been authenticated yet.
  """

primitive DataError
  """
  Error returned when result data from the server is in an unexpected format
  (e.g., column count mismatch across rows) or when a parameter value cannot
  be encoded for the wire protocol (e.g., a custom codec rejects a value).
  In either direction, the query is not delivered to the receiver as a result.
  """
