primitive _AuthenticationRequestType
  """
  Authentication request types

  See: https://www.postgresql.org/docs/current/protocol-message-formats.html
  """
  fun ok(): I32 =>
    0

  fun md5_password(): I32 =>
    5

  fun sasl(): I32 =>
    10

  fun sasl_continue(): I32 =>
    11

  fun sasl_final(): I32 =>
    12
