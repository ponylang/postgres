## Add SCRAM-SHA-256 authentication support

The driver now supports SCRAM-SHA-256 authentication, which has been the default PostgreSQL authentication method since version 10. The authentication method is negotiated automatically — no code changes are needed. Existing code that connects to PostgreSQL servers using SCRAM-SHA-256 will now authenticate successfully where it previously failed.

Two new `AuthenticationFailureReason` variants are available for more specific error handling:

```pony
be pg_session_authentication_failed(
  session: Session,
  reason: AuthenticationFailureReason)
=>
  match reason
  | InvalidPassword => // wrong credentials
  | InvalidAuthenticationSpecification => // bad username
  | ServerVerificationFailed => // server's SCRAM signature didn't match (possible MITM)
  | UnsupportedAuthenticationMethod => // server requested an unsupported auth method
  end
```

MD5 authentication continues to work as before.
