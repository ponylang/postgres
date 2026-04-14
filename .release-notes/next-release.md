## Fix crash on server rejection during startup

When PostgreSQL rejected a connection during startup with an error response — most commonly when `max_connections` had been exhausted — the driver crashed the process through an unreachable-state panic instead of delivering the failure to the application. Server rejections during startup now arrive through `pg_session_connection_failed` with the full `ErrorResponseMessage` available for inspection.

## Consolidate authentication failure into connection failure

The `pg_session_authentication_failed` callback has been removed. All pre-ready failures — transport-level errors, TLS errors, unsupported authentication methods, bad passwords, and server rejections during startup — now arrive through the single `pg_session_connection_failed` callback.

This matches how other PostgreSQL clients describe startup failures and ensures every startup error reaches the application through one well-defined path. `ConnectionFailureReason` has been expanded to include all the variants previously covered by `AuthenticationFailureReason`, plus new variants for specific server-rejection scenarios.

### Migration

Override `pg_session_connection_failed` instead of `pg_session_authentication_failed`. The `AuthenticationFailureReason` type has been removed; its members are now part of `ConnectionFailureReason`.

Before:

```pony
be pg_session_authentication_failed(session: Session,
  reason: AuthenticationFailureReason)
=>
  match reason
  | InvalidPassword => _out.print("Bad password")
  | InvalidAuthenticationSpecification => _out.print("Invalid user")
  | UnsupportedAuthenticationMethod => _out.print("Unsupported method")
  | ServerVerificationFailed => _out.print("Server verification failed")
  end

be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  _out.print("Connection failed")
```

After:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match reason
  | let r: InvalidPassword =>
    _out.print("Bad password: " + r.response().message)
  | let r: InvalidAuthorizationSpecification =>
    _out.print("Invalid user: " + r.response().message)
  | let r: TooManyConnections =>
    _out.print("Too many connections: " + r.response().message)
  | let r: InvalidDatabaseName =>
    _out.print("Database does not exist: " + r.response().message)
  | let r: ServerRejected =>
    _out.print("Server rejected startup (SQLSTATE "
      + r.response().code + "): " + r.response().message)
  | UnsupportedAuthenticationMethod => _out.print("Unsupported method")
  | ServerVerificationFailed => _out.print("Server verification failed")
  else
    _out.print("Connection failed")
  end
```

### Summary of API changes

- `pg_session_authentication_failed` is removed.
- `AuthenticationFailureReason` is removed; its members are part of `ConnectionFailureReason`.
- `InvalidAuthenticationSpecification` is renamed to `InvalidAuthorizationSpecification` (matches the official SQLSTATE 28000 name).
- `InvalidPassword` and `InvalidAuthorizationSpecification` are now `class val` wrappers around `ErrorResponseMessage`, accessed via `response()`. A match arm that was `| InvalidPassword =>` must become `| let r: InvalidPassword =>` to bind the value. Identity comparisons such as `reason is InvalidPassword` no longer match — two instances of the class are never `is`-equal; use a match arm with a type binding instead.
- `UnsupportedAuthenticationMethod` and `ServerVerificationFailed` remain primitives but are now delivered via `pg_session_connection_failed`.
- New variants: `TooManyConnections` (SQLSTATE 53300), `InvalidDatabaseName` (SQLSTATE 3D000), and `ServerRejected` (fallback for any other server ErrorResponse during startup). All three are `class val` wrappers around `ErrorResponseMessage`.
- `pg_session_shutdown` now fires after every `pg_session_connection_failed`. Previously, transport-level and TLS-negotiation failures fired only `pg_session_connection_failed` while authentication and server-rejection failures fired both. The unified failure callback now always terminates with `pg_session_shutdown`, matching the "session is torn down" mental model regardless of which phase failed.
