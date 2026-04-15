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

## Close SCRAM mutual-authentication bypass

The driver now rejects SCRAM-SHA-256 exchanges in which the server skips, duplicates, or malforms authentication messages. Previously, a server could send `AuthenticationOk` without a preceding `AuthenticationSASLFinal` and the driver would authenticate the session without verifying the server's signature, defeating SCRAM's mutual-authentication property.

Protocol violations during SCRAM — a skipped `AuthenticationSASLFinal`, a duplicated `AuthenticationSASLContinue`, an `AuthenticationSASLFinal` arriving before `AuthenticationSASLContinue`, malformed SASLFinal content, or a malformed or nonce-mismatched `AuthenticationSASLContinue` — now fail the connection via `pg_session_connection_failed` with `ServerVerificationFailed`, matching the existing behavior for a mismatched server signature. Previously, several of these conditions caused the session to close silently without notifying the application.

## Deliver server protocol violations to the application

A server can send bytes the driver can't parse, a wire-legal message that's invalid for the current connection state, or an unexpected byte during SSL negotiation. Any of those used to silently shut the session down or, worse, crash the client process through an illegal-state panic. Neither outcome gave an application trying to understand why its session died anything to work with.

All three paths now route through the state machine's own error handling. A pre-ready violation fires `pg_session_connection_failed(ProtocolViolation)` followed by `pg_session_shutdown`. A logged-in session with a query in flight delivers `ProtocolViolation` to that query's receiver — `pg_query_failed`, `pg_prepare_failed`, `pg_copy_failed`, `pg_stream_failed`, or `pg_pipeline_failed` — before `pg_session_shutdown` fires. Queries that were merely queued still receive `SessionClosed`, since only the in-flight query directly observed the violation.

For a pipeline, the currently-executing query receives `ProtocolViolation` and the remaining queries receive `SessionClosed`.

## Add ProtocolViolation to ConnectionFailureReason and ClientQueryError

`ProtocolViolation` is a new primitive that now appears in both the `ConnectionFailureReason` union (delivered via `pg_session_connection_failed`) and the `ClientQueryError` union (delivered via `pg_query_failed` and its peers). It carries no diagnostic payload. Shipping server-supplied bytes or parser state with the failure would be an attack vector for log injection, DoS amplification, and running code on hostile input during error handling. Easier to add bounded symbolic detail later if a user need emerges than to remove it once shipped.

### Migration

Any `match \exhaustive\` on `ConnectionFailureReason` or `ClientQueryError` needs a new arm for `ProtocolViolation`. Non-exhaustive matches (with an `else` clause or no `\exhaustive\` annotation) keep compiling without changes.

Before:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match \exhaustive\ reason
  | ConnectionFailedDNS => _out.print("DNS")
  | ConnectionFailedTCP => _out.print("TCP")
  // ...
  end
```

After:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match \exhaustive\ reason
  | ConnectionFailedDNS => _out.print("DNS")
  | ConnectionFailedTCP => _out.print("TCP")
  // ...
  | ProtocolViolation => _out.print("Protocol violation")
  end
```

The same applies to `ClientQueryError`: add a `| ProtocolViolation =>` arm to any exhaustive match and handle the case the way you'd handle an unrecoverable session failure on the query you dispatched.
## Guard against integer underflow on server-supplied message lengths

When a PostgreSQL server declared a message length smaller than the protocol's minimum, the driver performed an unsigned subtraction that wrapped to a huge value. The full consequences of the wrap were not fully characterized — one observed effect was silently consuming malformed bytes as a zero-payload acknowledgement before eventually reporting a protocol violation, but other downstream effects may have been reachable with different message shapes. The driver now validates length fields before arithmetic and rejects such messages as a protocol violation immediately.

## Detect peer-initiated TCP close during any session state

If the server closed the TCP connection at any point — during SSL negotiation, pre-auth startup, mid-SCRAM, or after the session reached the ready state — the driver would hang indefinitely without notifying the application. Peer close is now detected and delivered through the state machine's own error handling.

A pre-ready peer close fires `pg_session_connection_failed(ConnectionClosedByServer)` followed by `pg_session_shutdown`. A logged-in session with a query in flight delivers `SessionClosed` to that query's receiver — `pg_query_failed`, `pg_prepare_failed`, `pg_copy_failed`, `pg_stream_failed`, or `pg_pipeline_failed` — before `pg_session_shutdown` fires. Queued queries still receive `SessionClosed` through the existing shutdown drain.

## Add ConnectionClosedByServer to ConnectionFailureReason

`ConnectionClosedByServer` is a new primitive in the `ConnectionFailureReason` union (delivered via `pg_session_connection_failed`). It indicates the server closed the TCP connection before the session reached the ready state. It carries no payload — there is nothing useful to attach to a peer-initiated close.

This is distinct from `ConnectionFailedTCP`, which signals that the TCP connection could never be established in the first place. `ConnectionClosedByServer` means the server accepted the connection and then closed it.

### Migration

Any `match \exhaustive\` on `ConnectionFailureReason` needs a new arm for `ConnectionClosedByServer`. Non-exhaustive matches (with an `else` clause or no `\exhaustive\` annotation) keep compiling without changes. `ClientQueryError` is intentionally unchanged — post-ready peer close surfaces through the existing `SessionClosed` variant.

Before:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match \exhaustive\ reason
  | ConnectionFailedDNS => _out.print("DNS")
  | ConnectionFailedTCP => _out.print("TCP")
  // ...
  end
```

After:

```pony
be pg_session_connection_failed(session: Session,
  reason: ConnectionFailureReason)
=>
  match \exhaustive\ reason
  | ConnectionFailedDNS => _out.print("DNS")
  | ConnectionFailedTCP => _out.print("TCP")
  // ...
  | ConnectionClosedByServer => _out.print("Server closed connection")
  end
```
## Guard against integer overflow on server-supplied message lengths

On 32-bit platforms, a PostgreSQL server that declared a message length near `U32.max` could wrap the driver's internal size calculation to a small value (including 0). The buffer-size check then passed incorrectly and the parser could return a phantom acknowledgement message — a bogus success. The driver now validates the size arithmetic and rejects such messages as a protocol violation immediately. 64-bit platforms were not affected.

## Fix statement timeout dropped when timer event subscription fails

A statement timeout could silently disappear if the kernel returned an error when the driver's internal timer tried to register itself with the I/O event loop (for example, `ENOMEM` under sustained resource pressure). The operation would then run without a timeout even though one had been requested. The driver now detects this failure and rearms the timer with the original duration, so a transient registration failure no longer drops the timeout. Recovery is best-effort: if the rearm itself fails to register, the timeout is still lost for that operation.

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

