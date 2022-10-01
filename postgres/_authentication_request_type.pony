primitive _AuthenticationRequestType
  """
  Authentication request types

  See: https://www.postgresql.org/docs/current/protocol-message-formats.html
  """
  fun ok(): I32 =>
    0

  fun md5_password(): I32 =>
    5
