primitive AllowAnyAuth
  """
  The client accepts any authentication method the server offers, including
  no authentication. Use this to connect to servers configured with MD5,
  cleartext, or trust authentication. Opt-in only — the default
  (`AuthRequireSCRAM`) rejects these weaker methods.
  """

primitive AuthRequireSCRAM
  """
  The client requires SCRAM-SHA-256 authentication. Connection fails with
  `pg_session_connection_failed(AuthenticationMethodRejected)` if the server
  responds to the startup message with any other authentication challenge
  (cleartext, MD5, or no authentication). This is the default.

  SCRAM is the only PostgreSQL authentication method that provides mutual
  authentication — the client verifies the server knows the password, not
  just that the server accepted the client's proof. Requiring SCRAM closes
  downgrade attacks where a malicious or compromised server asks for a
  weaker method.
  """

type AuthRequirement is (AllowAnyAuth | AuthRequireSCRAM)
