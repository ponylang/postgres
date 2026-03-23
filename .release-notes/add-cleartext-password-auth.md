## Add cleartext password authentication

Sessions can now authenticate to PostgreSQL servers that require cleartext password authentication. Previously, connecting to such a server would fire `pg_session_authentication_failed` with `UnsupportedAuthenticationMethod`.

No API changes are needed. The driver detects the server's requested authentication method and sends the password from `DatabaseConnectInfo` automatically, the same as it does for MD5 and SCRAM-SHA-256.
