## Fix session close dropping queued writes

When a session hands the socket more bytes than it will take at once, the rest is queued until the socket drains. `Session.close()` discarded that queue — the bytes never reached the server, even though the session was closed cleanly rather than aborted.

`Session.close()` now writes out what is still queued before shutting the session down.

## Fix a hang when closing a session while handling a server message

postgres closes a session from inside its handling of a message that just arrived: when the server sends something that violates the protocol, and when a connection attempt fails after the server has already replied. Closing there could leave the session attempting one more read on the socket it had just closed. In a program that opens and closes many sessions, that stray read could block one of the runtime's scheduler threads and keep the program from exiting.

A session no longer attempts that read after closing.

## Fix a possible write hang under load

Under write load, a write on a session's socket could stall and never return, stopping the whole program. The stall needed a blocking file descriptor, and sessions use non-blocking ones, so it was only reachable once the operating system had reused a closed connection's descriptor number for a blocking socket somewhere else in the same process — rare, but possible.

Writes no longer hang on Linux, FreeBSD, OpenBSD, and DragonFly. macOS and Windows are unchanged.

## Require ponyc 0.67.0 or later

postgres now requires ponyc 0.67.0 or later on every platform. The previous minimum was 0.64.0, and 0.66.0 on Windows; 0.64.0 through 0.66.x are no longer supported. The write hang under load is closed by a socket call that ponyc added in 0.67.0.

## Move to ponylang/ssl 3.0.0

postgres now depends on ponylang/ssl 3.0.0, where it depended on 2.0.1. Nothing in postgres's own API changed — you still build an `SSLContext val` and hand it to `SSLRequired` or `SSLPreferred`. If your application does not declare ssl itself, it picks up 3.0.0 through postgres, and your own `use "ssl/net"` or `use "ssl/crypto"` code is built against it. If your application does declare ssl, your declaration is the one that applies.

The one most likely to reach you is a rename. The twelve protocol-version primitives now spell their acronyms in full, and those are the values passed to `SSLContext.set_min_proto_version` and `set_max_proto_version` — so pinning a minimum TLS version on the context you hand postgres needs an edit.

```pony
// Before
ctx.set_min_proto_version(Tls1u2Version())?

// After
ctx.set_min_proto_version(TLS1u2Version())?
```

The rest of the breaking changes: constructing a `Digest`, `Digest.final`, and `HmacSha256` are now partial and need a `?`; `SSLContext.alpn_set_resolver` takes an `ALPNProtocolResolver val` where it took a `box`; `SSLState` gained an `SSLDisposed` member, which breaks an exhaustive match on `SSL.state()`; and a reference typed `HashFn tag` no longer compiles, so type it `val` or `box`. See ponylang/ssl's own 3.0.0 release notes for the details.
