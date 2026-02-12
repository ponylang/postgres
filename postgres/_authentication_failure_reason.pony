type AuthenticationFailureReason is
  ( InvalidAuthenticationSpecification
  | InvalidPassword
  | ServerVerificationFailed
  | UnsupportedAuthenticationMethod )

primitive InvalidAuthenticationSpecification
primitive InvalidPassword

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
