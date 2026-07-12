# postgres

Pure Pony PostgreSQL driver. Beta-level.

<!-- contributor-only -->
## Contributing with an AI assistant

This is a Pony project. The ponylang org maintains a set of LLM coding skills. Get set up with them before contributing:

- **Not set up yet?** Install them once:

  ```bash
  git clone https://github.com/ponylang/llm-skills.git
  cd llm-skills
  python install.py
  ```

- **Already set up?** Make sure you're on the latest. If you installed with the script above, `git pull` in the directory where you cloned `llm-skills` and the symlinked skills update automatically — if you set them up another way, refresh them however that setup expects.

See the [llm-skills README](https://github.com/ponylang/llm-skills) for details and other harnesses.

When you start working on this project, load the `pony-skills` skill — it tells your assistant which Pony skill to use for each task.

Read [CONTRIBUTING.md](CONTRIBUTING.md).
<!-- /contributor-only -->

## Building and testing

```
make ssl=3.0.x                     # build + run all tests
make unit-tests ssl=3.0.x          # unit tests only (no PostgreSQL needed)
make test-one t=TestName ssl=3.0.x # run a single test by name
make integration-tests ssl=3.0.x   # integration tests (needs the containers below)
make examples ssl=3.0.x            # compile examples
make start-pg-containers           # start the test PostgreSQL containers
make stop-pg-containers            # stop them
```

`ssl=` is required (for example `ssl=3.0.x` for OpenSSL 3.x). Tests run `--sequential`.

Integration tests need two PostgreSQL 14.5 containers, started by `make start-pg-containers`: a plaintext one on port 5432 (SCRAM-SHA-256 default auth) and an SSL one on port 5433. Connection parameters come from the `POSTGRES_*` environment variables read by `_ConnectionTestConfiguration` in `_test.pony`.

## Architecture

The `Session` actor is the entry point. It implements `lori.TCPConnectionActor` and `lori.ClientLifecycleEventReceiver` and tracks its lifecycle with explicit `_SessionState` classes. Session state is composed from a trait hierarchy that supplies each state's default responses, so a concrete state writes only the transitions that differ from those defaults; the rest fall through to a trait default — a panic (`_IllegalState()`), a protocol-violation handler, or a deliberate no-op, depending on the state.

```
_SessionUnopened  --connect (no SSL)-->              _SessionConnected
_SessionUnopened  --connect (SSLRequired/Preferred)--> _SessionSSLNegotiating
_SessionUnopened  --fail-->                            _SessionClosed
_SessionSSLNegotiating --'S'+TLS ok-->                 _SessionConnected
_SessionSSLNegotiating --'N' (SSLRequired)-->          _SessionClosed
_SessionSSLNegotiating --'N' (SSLPreferred)-->         _SessionConnected  (plaintext fallback)
_SessionSSLNegotiating --TLS fail-->                   _SessionClosed
_SessionConnected --cleartext auth ok-->                _SessionLoggedIn
_SessionConnected --cleartext auth fail-->             _SessionClosed
_SessionConnected --MD5 auth ok-->                     _SessionLoggedIn
_SessionConnected --MD5 auth fail-->                   _SessionClosed
_SessionConnected --SASL challenge-->                  _SessionSCRAMAuthenticating
_SessionSCRAMAuthenticating --auth ok-->               _SessionLoggedIn
_SessionSCRAMAuthenticating --auth fail-->             _SessionClosed
_SessionLoggedIn  --close-->                           _SessionClosed
(protocol violation)* --on_protocol_violation-->       _SessionClosed
(peer TCP close)*     --on_closed-->                   _SessionClosed
```

Once logged in, one operation runs at a time: `_SessionLoggedIn` queues operations and a `_QueryState` sub-state-machine drives each through its wire exchange, dequeuing on `ReadyForQuery`. Query cancellation opens a second TCP connection (`_CancelSender`), because PostgreSQL requires the `CancelRequest` on a different connection from the one running the query. Design: [discussion #88](https://github.com/ponylang/postgres/discussions/88).

Codec design: [discussion #139](https://github.com/ponylang/postgres/discussions/139). Feature roadmap: [discussion #72](https://github.com/ponylang/postgres/discussions/72).

## Security constraints

- **SSLRequest is sent only after the connection is set to buffer reads (`buffer_until`) (CVE-2021-23222).** Otherwise plaintext arriving before the TLS upgrade would be read across the boundary — an injection vector. Design: [discussion #76](https://github.com/ponylang/postgres/discussions/76).
- **`auth_requirement` defaults to `AuthRequireSCRAM`.** The default rejects cleartext, MD5, and trust auth with `AuthenticationMethodRejected`, closing the server-driven downgrade vector; a caller talking to an MD5, cleartext, or trust server must opt in with `AllowAnyAuth`. The check lives in the `_AuthenticableState` trait defaults, so every authenticable state carries it unless it explicitly overrides. Design: [discussion #83](https://github.com/ponylang/postgres/discussions/83); downgrade defense: [issue #210](https://github.com/ponylang/postgres/issues/210).

## Conventions

- Impossible states call `_IllegalState()` / `_Unreachable()` (`_mort.pony`) rather than erroring or ignoring.
- Tests live in the `postgres/` package as private classes, all registered in the single `Main` runner in `_test.pony`.
- Mock-server tests bind ports in 7669–7763 and 9667–9668. **Do not use port 7680** — Windows reserves it (Update Delivery Optimization) and it fails to bind on WSL2.
- `\nodoc\` on test classes.

## PostgreSQL wire protocol

Message formats, the extended query protocol, and type OIDs are defined in the [PostgreSQL 14 protocol docs](https://www.postgresql.org/docs/14/protocol.html) — see [message formats](https://www.postgresql.org/docs/14/protocol-message-formats.html) and [message flow](https://www.postgresql.org/docs/14/protocol-flow.html).
