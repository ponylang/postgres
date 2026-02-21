## Add SSLPreferred mode

`SSLPreferred` is a new SSL mode equivalent to PostgreSQL's `sslmode=prefer`. It attempts SSL negotiation when connecting and falls back to plaintext if the server refuses. A TLS handshake failure (server accepts but handshake fails) is a hard failure — the connection is not retried as plaintext.

Use `SSLPreferred` when you want encryption if available but don't want to fail when connecting to servers that don't support SSL:

```pony
use "ssl/net"

let sslctx = recover val
  SSLContext
    .> set_client_verify(false)
    .> set_server_verify(false)
end

let session = Session(
  ServerConnectInfo(auth, host, port, SSLPreferred(sslctx)),
  DatabaseConnectInfo(user, password, database),
  notify)
```

The existing `SSLRequired` mode is unchanged — it still aborts if the server refuses SSL.
