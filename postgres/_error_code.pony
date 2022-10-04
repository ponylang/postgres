primitive _ErrorCode
  """
  Error response codes.

  See: https://www.postgresql.org/docs/current/errcodes-appendix.html
  """
  fun invalid_authentication_specification(): String =>
    "28000"

  fun invalid_password(): String =>
    "28P01"
