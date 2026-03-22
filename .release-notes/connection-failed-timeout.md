## Add ConnectionFailedTimeout to ConnectionFailureReason

`ConnectionFailureReason` now includes `ConnectionFailedTimeout` for when a connection attempt times out before a TCP or TLS connection is established. If you have an exhaustive match on `ConnectionFailureReason`, you'll need to add the new arm:

Before:

```pony
match reason
| ConnectionFailedDNS => _env.out.print("DNS resolution failed")
| ConnectionFailedTCP => _env.out.print("TCP connection failed")
| SSLServerRefused => _env.out.print("Server refused SSL")
| TLSAuthFailed => _env.out.print("TLS certificate error")
| TLSHandshakeFailed => _env.out.print("TLS handshake failed")
end
```

After:

```pony
match reason
| ConnectionFailedDNS => _env.out.print("DNS resolution failed")
| ConnectionFailedTCP => _env.out.print("TCP connection failed")
| SSLServerRefused => _env.out.print("Server refused SSL")
| TLSAuthFailed => _env.out.print("TLS certificate error")
| TLSHandshakeFailed => _env.out.print("TLS handshake failed")
| ConnectionFailedTimeout => _env.out.print("Connection timed out")
end
```
