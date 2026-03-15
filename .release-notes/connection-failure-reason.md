## Change pg_session_connection_failed to include a failure reason

`pg_session_connection_failed` on `SessionStatusNotify` now takes a `ConnectionFailureReason` parameter indicating why the connection failed. This is a closed union type enabling exhaustive matching:

Before:

```pony
be pg_session_connection_failed(session: Session) =>
  _env.out.print("Connection failed")
```

After:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match reason
  | ConnectionFailedDNS => _env.out.print("DNS resolution failed")
  | ConnectionFailedTCP => _env.out.print("TCP connection failed")
  | SSLServerRefused => _env.out.print("Server refused SSL")
  | TLSAuthFailed => _env.out.print("TLS certificate error")
  | TLSHandshakeFailed => _env.out.print("TLS handshake failed")
  end
```
