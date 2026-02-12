## Fix unsupported authentication type causing silent hang

When a PostgreSQL server requested an authentication method the driver doesn't support (e.g., cleartext password, Kerberos, GSSAPI), the session would hang indefinitely with no error reported. It now correctly fails with `UnsupportedAuthenticationMethod` via the `pg_session_authentication_failed` callback.
