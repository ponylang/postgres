type AuthenticationFailureReason is
  ( InvalidAuthenticationSpecification
  | InvalidPassword
  | ServerVerificationFailed
  | UnsupportedAuthenticationMethod )

primitive InvalidAuthenticationSpecification
  """
  The server rejected the connection due to an invalid authentication
  specification (SQLSTATE 28000), such as a nonexistent user or a user
  not permitted to connect to the requested database.
  """

primitive InvalidPassword
  """
  The server rejected the provided password (SQLSTATE 28P01).
  """

primitive ServerVerificationFailed
  """
  The server's SCRAM signature did not match the expected value. This may
  indicate a man-in-the-middle attack or a misconfigured server.
  """

primitive UnsupportedAuthenticationMethod
  """
  The server requested an authentication method that this driver does not
  support.
  """
