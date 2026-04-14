## Close SCRAM mutual-authentication bypass

The driver now rejects SCRAM-SHA-256 exchanges in which the server skips, duplicates, or malforms authentication messages. Previously, a server could send `AuthenticationOk` without a preceding `AuthenticationSASLFinal` and the driver would authenticate the session without verifying the server's signature, defeating SCRAM's mutual-authentication property.

Protocol violations during SCRAM — a skipped `AuthenticationSASLFinal`, a duplicated `AuthenticationSASLContinue`, an `AuthenticationSASLFinal` arriving before `AuthenticationSASLContinue`, malformed SASLFinal content, or a malformed or nonce-mismatched `AuthenticationSASLContinue` — now fail the connection via `pg_session_connection_failed` with `ServerVerificationFailed`, matching the existing behavior for a mismatched server signature. Previously, several of these conditions caused the session to close silently without notifying the application.
