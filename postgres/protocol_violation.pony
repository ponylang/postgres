primitive ProtocolViolation
  """
  The server sent data that could not be parsed as a valid PostgreSQL wire
  protocol message, was a well-formed message of a type invalid in the
  current connection state, or sent an unexpected byte during SSL
  negotiation. The session has been shut down because recovery is not
  possible.
  """
