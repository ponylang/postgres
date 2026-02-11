## Add SSL/TLS negotiation support

You can now encrypt connections to PostgreSQL using SSL/TLS. Pass `SSLRequired(sslctx)` to `Session.create()` to enable SSL negotiation before authentication. The default `SSLDisabled` preserves the existing plaintext behavior.

```pony
use "ssl/net"
use "postgres"

// Create an SSLContext (configure certificates/verification as needed)
let sslctx = recover val
  SSLContext
    .> set_client_verify(false)
    .> set_server_verify(false)
end

// Connect with SSL
let session = Session(
  auth,
  notify,
  host,
  port,
  username,
  password,
  database,
  SSLRequired(sslctx))
```

If the server accepts SSL, the connection is encrypted before authentication begins. If the server refuses, `pg_session_connection_failed` fires.
