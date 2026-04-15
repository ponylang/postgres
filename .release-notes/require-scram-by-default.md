## Require SCRAM authentication by default

`ServerConnectInfo` now requires SCRAM-SHA-256 authentication by default. The driver refuses connections to servers configured for MD5, cleartext, or trust authentication with `pg_session_connection_failed(AuthenticationMethodRejected)` unless the application explicitly opts in to `AllowAnyAuth`.

SCRAM-SHA-256 is the only PostgreSQL authentication method that verifies the server knows the password — the client gets mutual authentication, not just a successful server-side password check. Previously, the driver accepted whatever authentication method the server requested, including `AuthenticationOk` with no challenge, which let a malicious or compromised server downgrade the exchange to a scheme that does not prove the server's identity. Defending against that downgrade requires a client-side policy; secure-by-default closes the vector for applications that don't know to ask.

### Migration

Applications connecting to a server that offers MD5, cleartext, or trust authentication now see `pg_session_connection_failed(AuthenticationMethodRejected)` where they previously saw successful authentication. To preserve the old behavior, construct `ServerConnectInfo` with `AllowAnyAuth`.

Before:

```pony
let info = ServerConnectInfo(
  lori.TCPConnectAuth(env.root), "localhost", "5432")
```

After (when the server uses MD5, cleartext, or trust):

```pony
let info = ServerConnectInfo(
  lori.TCPConnectAuth(env.root), "localhost", "5432",
  SSLDisabled, AllowAnyAuth)
```

### Summary of API changes

- New `AuthRequirement` union: `(AllowAnyAuth | AuthRequireSCRAM)`. Client policy constraining which server-offered authentication methods the driver will accept.
- `AllowAnyAuth` — primitive opting into the pre-change behavior.
- `AuthRequireSCRAM` — primitive requiring SCRAM-SHA-256. This is the new default.
- `AuthenticationMethodRejected` — new `ConnectionFailureReason` variant delivered when the server-offered method is disallowed by the session's `AuthRequirement`. Distinct from `UnsupportedAuthenticationMethod`, which indicates the driver cannot perform the requested method at all.
- `ServerConnectInfo.create` has a new `auth_requirement'` parameter between `ssl_mode'` and `connection_timeout'`. Callers that pass `connection_timeout` by name are unaffected.
