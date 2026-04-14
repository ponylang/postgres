use lori = "lori"
use "pony_test"

// Tests for server-driven protocol violations that are detected by the
// client and surface as `ProtocolViolation`. See
// `.release-notes/next-release.md` for the user-facing summary.
//
// The inline `_TestHandlingJunkMessages` test covers pre-auth parse
// failure (`_SessionConnected`), and `_TestSSLNegotiationJunkResponse`
// covers the SSL-negotiation junk-byte branch. The tests in this file
// cover: parse failure during SCRAM, parse failure in `_SessionLoggedIn`
// (with and without an in-flight query), a wrong-state message during
// `_SessionConnected`, a wrong-state message during SCRAM, wrong-state
// messages during `_SessionLoggedIn`, and per-receiver coverage for each
// in-flight state (simple + extended query, close statement, copy in,
// copy out, prepare, stream, pipeline).
//
// Known gap: the narrow window between `AuthenticationOk` and the first
// `ReadyForQuery` — where `query_state == _QueryNotReady` — is not
// exercised directly. `_QueryNotReady` and `_QueryReady` share the same
// `_QueryNoQueryInFlight.on_protocol_violation` no-op default; the
// `ParseIdle` and `WrongStateIdle` tests exercise the `_QueryReady` case,
// and `_QueryNotReady` follows the same code path.

class \nodoc\ iso _TestProtocolViolationParseInSCRAM is UnitTest
  """
  Parse failure during `_SessionSCRAMAuthenticating`. Asserts
  `pg_session_connection_failed(ProtocolViolation)` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/ParseDuringSCRAM"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7737"

    let listener = _PVParseSCRAMListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVParseSCRAMNotify is SessionStatusNotify
  let _h: TestHelper
  var _failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ProtocolViolation => _failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _failed then
      _h.fail("pg_session_shutdown before pg_session_connection_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVParseSCRAMListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseSCRAMServer =>
    let server = _PVParseSCRAMServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseSCRAMNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVParseSCRAMServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Starts a SASL SCRAM-SHA-256 exchange, then sends unparseable bytes
  after the client's initial response to force a parse failure while
  the session is in `_SessionSCRAMAuthenticating`.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _phase: USize = 0

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if _phase == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _phase = 1
        let mechanisms: Array[String] val = recover val
          ["SCRAM-SHA-256"] end
        _tcp_connection.send(
          _IncomingAuthenticationSASLTestMessage(mechanisms).bytes())
      end
    elseif _phase == 1 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        _phase = 2
        _tcp_connection.send(_IncomingJunkTestMessage.bytes())
      end
    end

class \nodoc\ iso _TestProtocolViolationParseInFlight is UnitTest
  """
  Parse failure during `_SessionLoggedIn` with an in-flight query.
  Asserts `pg_query_failed(ProtocolViolation)` before
  `pg_session_shutdown` (counterfactual: ordering is explicit).
  """
  fun name(): String =>
    "ProtocolViolation/ParseInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7731"

    let listener = _PVParseInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVParseInFlightClient is
  (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query: SimpleQuery = SimpleQuery("SELECT 1")
  var _query_failed: Bool = false
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.execute(_query, this)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected pg_query_failed, not pg_query_result.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _query_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _query_failed then
      _h.fail("pg_session_shutdown fired before pg_query_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVParseInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVParseInFlightServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, waits for a SimpleQuery, then replies with
  unparseable bytes to force a parse failure while a query is in flight.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if not _authed then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _authed = true
        _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
        _tcp_connection.send(_IncomingReadyForQueryTestMessage('I').bytes())
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        _tcp_connection.send(_IncomingJunkTestMessage.bytes())
      end
    end

class \nodoc\ iso _TestProtocolViolationParseIdle is UnitTest
  """
  Parse failure during `_SessionLoggedIn` with no in-flight query.
  Asserts `pg_session_shutdown` without `pg_query_failed`.
  """
  fun name(): String =>
    "ProtocolViolation/ParseIdle"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7732"

    let listener = _PVParseIdleListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVParseIdleClient is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _authenticated = true

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail(
        "pg_session_shutdown fired without reaching the logged-in state.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVParseIdleListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseIdleServer =>
    let server = _PVParseIdleServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseIdleClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVParseIdleServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, signals ReadyForQuery, then pushes junk
  while the session is idle (no query in flight).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
      _tcp_connection.send(_IncomingReadyForQueryTestMessage('I').bytes())
      _tcp_connection.send(_IncomingJunkTestMessage.bytes())
    end

class \nodoc\ iso _TestProtocolViolationWrongStatePreAuth is UnitTest
  """
  Wrong-state server message during `_SessionConnected` (pre-auth):
  server sends `DataRow` before any authentication message. Asserts
  `pg_session_connection_failed(ProtocolViolation)` then
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/WrongStatePreAuth"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7733"

    let listener = _PVWrongStatePreAuthListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVWrongStatePreAuthListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVWrongStatePreAuthServer =>
    let server = _PVWrongStatePreAuthServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseSCRAMNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVWrongStatePreAuthServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Replies to the StartupMessage with a `DataRow`, which is invalid
  pre-auth.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _sent: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if _sent then return end
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _sent = true
      let cols: Array[(String | None)] val = recover val
        [as (String | None): "1"] end
      _tcp_connection.send(_IncomingDataRowTestMessage(cols).bytes())
    end

class \nodoc\ iso _TestProtocolViolationWrongStateSCRAM is UnitTest
  """
  Wrong-state server message during SCRAM: mid-SCRAM the server sends
  `AuthenticationCleartextPassword`, which is invalid when a SASL
  exchange is in progress (the motivating example from #208). Asserts
  `pg_session_connection_failed(ProtocolViolation)` then
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/WrongStateSCRAM"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7734"

    let listener = _PVWrongStateSCRAMListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVWrongStateSCRAMListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVWrongStateSCRAMServer =>
    let server = _PVWrongStateSCRAMServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseSCRAMNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVWrongStateSCRAMServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Begins SCRAM-SHA-256, then sends `AuthenticationCleartextPassword`
  mid-exchange.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _phase: USize = 0

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if _phase == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _phase = 1
        let mechanisms: Array[String] val = recover val
          ["SCRAM-SHA-256"] end
        _tcp_connection.send(
          _IncomingAuthenticationSASLTestMessage(mechanisms).bytes())
      end
    elseif _phase == 1 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        _phase = 2
        _tcp_connection.send(
          _IncomingAuthenticationCleartextPasswordTestMessage.bytes())
      end
    end

class \nodoc\ iso _TestProtocolViolationWrongStateIdle is UnitTest
  """
  Wrong-state server message during `_SessionLoggedIn` with no in-flight
  query: server sends `AuthenticationOk` to an idle session. Asserts
  `pg_session_shutdown` (no `pg_query_failed` — nothing in flight).
  """
  fun name(): String =>
    "ProtocolViolation/WrongStateIdle"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7735"

    let listener = _PVWrongStateIdleListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVWrongStateIdleListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVWrongStateIdleServer =>
    let server = _PVWrongStateIdleServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseIdleClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVWrongStateIdleServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, signals ReadyForQuery, then pushes a second
  `AuthenticationOk` while the session is idle.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
      _tcp_connection.send(_IncomingReadyForQueryTestMessage('I').bytes())
      _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
    end

class \nodoc\ iso _TestProtocolViolationWrongStateInFlight is UnitTest
  """
  Wrong-state server message during `_SessionLoggedIn` with an in-flight
  query: server responds to a SimpleQuery with `AuthenticationOk` rather
  than query-result messages. Asserts `pg_query_failed(ProtocolViolation)`
  before `pg_session_shutdown` (counterfactual: ordering is explicit).
  """
  fun name(): String =>
    "ProtocolViolation/WrongStateInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7736"

    let listener = _PVWrongStateInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVWrongStateInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVWrongStateInFlightServer =>
    let server = _PVWrongStateInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVParseInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PVWrongStateInFlightServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, waits for a SimpleQuery, then replies with
  `AuthenticationOk` — a wire-legal message that is invalid in
  `_SessionLoggedIn`.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if not _authed then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _authed = true
        _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
        _tcp_connection.send(_IncomingReadyForQueryTestMessage('I').bytes())
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
      end
    end

class \nodoc\ iso _TestProtocolViolationCopyInInFlight is UnitTest
  """
  Parse failure during an in-flight `copy_in` (state
  `_CopyInInFlight`). Asserts `pg_copy_failed(ProtocolViolation)` on the
  `CopyInReceiver` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/CopyInInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7738"

    let listener = _PVCopyInInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVCopyInInFlightClient is
  (SessionStatusNotify & CopyInReceiver)
  let _h: TestHelper
  var _copy_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.copy_in("COPY t FROM STDIN", this)

  be pg_copy_ready(session: Session) =>
    _h.fail("Expected pg_copy_failed, not pg_copy_ready.")
    _h.complete(false)

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Expected pg_copy_failed, not pg_copy_complete.")
    _h.complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _copy_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _copy_failed then
      _h.fail("pg_session_shutdown fired before pg_copy_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVCopyInInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVCopyInInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationCopyOutInFlight is UnitTest
  """
  Parse failure during an in-flight `copy_out` (state
  `_CopyOutInFlight`). Asserts `pg_copy_failed(ProtocolViolation)` on the
  `CopyOutReceiver` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/CopyOutInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7739"

    let listener = _PVCopyOutInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVCopyOutInFlightClient is
  (SessionStatusNotify & CopyOutReceiver)
  let _h: TestHelper
  var _copy_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.copy_out("COPY t TO STDOUT", this)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _h.fail("Expected pg_copy_failed, not pg_copy_data.")
    _h.complete(false)

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Expected pg_copy_failed, not pg_copy_complete.")
    _h.complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _copy_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _copy_failed then
      _h.fail("pg_session_shutdown fired before pg_copy_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVCopyOutInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVCopyOutInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationPrepareInFlight is UnitTest
  """
  Parse failure during an in-flight `prepare` (state `_PrepareInFlight`).
  Asserts `pg_prepare_failed(ProtocolViolation)` on the `PrepareReceiver`
  before `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/PrepareInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7740"

    let listener = _PVPrepareInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVPrepareInFlightClient is
  (SessionStatusNotify & PrepareReceiver)
  let _h: TestHelper
  var _prepare_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.prepare("s1", "SELECT 1", this)

  be pg_statement_prepared(session: Session, name: String) =>
    _h.fail("Expected pg_prepare_failed, not pg_statement_prepared.")
    _h.complete(false)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _prepare_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _prepare_failed then
      _h.fail("pg_session_shutdown fired before pg_prepare_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVPrepareInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVPrepareInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationStreamInFlight is UnitTest
  """
  Parse failure during an in-flight `stream` (state
  `_StreamingQueryInFlight`). Asserts `pg_stream_failed(ProtocolViolation)`
  on the `StreamingResultReceiver` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/StreamInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7741"

    let listener = _PVStreamInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVStreamInFlightClient is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper
  let _query: PreparedQuery = PreparedQuery("SELECT 1", recover val [] end)
  var _stream_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.stream(_query, 10, this)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _h.fail("Expected pg_stream_failed, not pg_stream_batch.")
    _h.complete(false)

  be pg_stream_complete(session: Session) =>
    _h.fail("Expected pg_stream_failed, not pg_stream_complete.")
    _h.complete(false)

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _stream_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end
    // Calling fetch_more and close_stream after the stream has failed is
    // expected to be a safe no-op — the session is (or is about to be)
    // closed and the `_UnconnectedState` defaults ignore these calls. If
    // either call produced a later `pg_stream_batch` or `pg_stream_complete`
    // it would fail the test via the earlier handlers.
    session.fetch_more()
    session.close_stream()

  be pg_session_shutdown(s: Session) =>
    if not _stream_failed then
      _h.fail("pg_session_shutdown fired before pg_stream_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVStreamInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVStreamInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationPipelineInFlight is UnitTest
  """
  Parse failure during an in-flight `pipeline` (state `_PipelineInFlight`).
  With two queries pipelined, asserts that the currently-executing query
  (index 0) receives `pg_pipeline_failed(ProtocolViolation)` and the
  second (index 1) receives `pg_pipeline_failed(SessionClosed)` from
  `drain_in_flight`, followed by `pg_pipeline_complete`, then
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "ProtocolViolation/PipelineInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7742"

    let listener = _PVPipelineInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVPipelineInFlightClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _got_protocol_violation: Bool = false
  var _got_session_closed: Bool = false
  var _got_complete: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    let q1 = PreparedQuery("SELECT 1", recover val [] end)
    let q2 = PreparedQuery("SELECT 2", recover val [] end)
    let queries: Array[(PreparedQuery | NamedPreparedQuery)] val =
      recover val [as (PreparedQuery | NamedPreparedQuery): q1; q2] end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _h.fail("Expected pg_pipeline_failed, not pg_pipeline_result.")
    _h.complete(false)

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match (index, failure)
    | (0, ProtocolViolation) => _got_protocol_violation = true
    | (1, SessionClosed) => _got_session_closed = true
    else
      _h.fail(
        "Unexpected pipeline failure at index " + index.string() + ".")
      _h.complete(false)
    end

  be pg_pipeline_complete(session: Session) =>
    if not (_got_protocol_violation and _got_session_closed) then
      _h.fail(
        "pg_pipeline_complete fired before all per-query failures.")
      _h.complete(false)
      return
    end
    _got_complete = true

  be pg_session_shutdown(s: Session) =>
    if not _got_complete then
      _h.fail("pg_session_shutdown fired before pg_pipeline_complete.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVPipelineInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVPipelineInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationExtendedQueryInFlight is UnitTest
  """
  Parse failure during an in-flight `PreparedQuery` execution (state
  `_ExtendedQueryInFlight`). The simple-query case and the extended-query
  case have duplicated notification logic — this test covers the extended
  side so a divergence in the copy doesn't pass CI.
  """
  fun name(): String =>
    "ProtocolViolation/ExtendedQueryInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7743"

    let listener = _PVExtendedQueryInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVExtendedQueryInFlightClient is
  (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query: PreparedQuery = PreparedQuery("SELECT 1", recover val [] end)
  var _query_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected pg_query_failed, not pg_query_result.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | ProtocolViolation => _query_failed = true
    else
      _h.fail("Expected ProtocolViolation.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _query_failed then
      _h.fail("pg_session_shutdown fired before pg_query_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVExtendedQueryInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVExtendedQueryInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestProtocolViolationCloseStatementInFlight is UnitTest
  """
  Parse failure during an in-flight `close_statement` (state
  `_CloseStatementInFlight`). `close_statement` is fire-and-forget with no
  receiver — the observable is a clean `pg_session_shutdown` without any
  crash. `_authenticated` is used as a precondition assertion so a
  regression that shut the session down pre-auth wouldn't pass falsely.
  """
  fun name(): String =>
    "ProtocolViolation/CloseStatementInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7744"

    let listener = _PVCloseStatementInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PVCloseStatementInFlightClient is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _authenticated = true
    session.close_statement("s1")

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail(
        "pg_session_shutdown fired without reaching the logged-in state.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _PVCloseStatementInFlightListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _PVParseInFlightServer =>
    let server = _PVParseInFlightServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PVCloseStatementInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)
