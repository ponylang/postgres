use "files"
use lori = "lori"
use "pony_test"
use "ssl/net"

// Tests for peer-initiated TCP close. Each test puts the session in a
// specific state, then the mock server closes the TCP connection; the
// client should report the closure through the appropriate callback
// rather than hang.

class \nodoc\ iso _TestRemoteCloseSSLNegotiating is UnitTest
  """
  Peer close during `_SessionSSLNegotiating`. Asserts
  `pg_session_connection_failed(ConnectionClosedByServer)` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/SSLNegotiating"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7745"

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _RemoteCloseSSLNegotiatingListener(
      lori.TCPListenAuth(h.env.root), host, port, h, sslctx)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseSSLNegotiatingNotify is SessionStatusNotify
  let _h: TestHelper
  var _failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ConnectionClosedByServer => _failed = true
    else
      _h.fail("Expected ConnectionClosedByServer.")
      _h.complete(false)
    end

  be pg_session_connected(s: Session) =>
    _h.fail("Should not reach connected during SSL negotiation close.")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    if not _failed then
      _h.fail("pg_session_shutdown before pg_session_connection_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseSSLNegotiatingListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _sslctx: SSLContext val

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper, sslctx: SSLContext val)
  =>
    _host = host
    _port = port
    _h = h
    _sslctx = sslctx
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _RemoteCloseAfterSSLRequestServer =>
    let server = _RemoteCloseAfterSSLRequestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLRequired(_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseSSLNegotiatingNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseAfterSSLRequestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Reads the client's SSLRequest, then closes without responding.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _closed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _closed then return end
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _closed = true
      _tcp_connection.close()
    end

class \nodoc\ iso _TestRemoteClosePreAuth is UnitTest
  """
  Peer close during `_SessionConnected` (pre-auth). Asserts
  `pg_session_connection_failed(ConnectionClosedByServer)` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/PreAuth"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7746"

    let listener = _RemoteClosePreAuthListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteClosePreAuthNotify is SessionStatusNotify
  let _h: TestHelper
  var _failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ConnectionClosedByServer => _failed = true
    else
      _h.fail("Expected ConnectionClosedByServer.")
      _h.complete(false)
    end

  be pg_session_authenticated(s: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    if not _failed then
      _h.fail("pg_session_shutdown before pg_session_connection_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteClosePreAuthListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterStartupServer =>
    let server = _RemoteCloseAfterStartupServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteClosePreAuthNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseAfterStartupServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Reads the client's StartupMessage, then closes without responding.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _closed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _closed then return end
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _closed = true
      _tcp_connection.close()
    end

class \nodoc\ iso _TestRemoteCloseSCRAM is UnitTest
  """
  Peer close during `_SessionSCRAMAuthenticating`. Server advances the
  SASL handshake to the point the client is mid-SCRAM, then closes.
  Asserts `pg_session_connection_failed(ConnectionClosedByServer)` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/SCRAM"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7747"

    let listener = _RemoteCloseSCRAMListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseSCRAMNotify is SessionStatusNotify
  let _h: TestHelper
  var _failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ConnectionClosedByServer => _failed = true
    else
      _h.fail("Expected ConnectionClosedByServer.")
      _h.complete(false)
    end

  be pg_session_authenticated(s: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    if not _failed then
      _h.fail("pg_session_shutdown before pg_session_connection_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseSCRAMListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseSCRAMServer =>
    let server = _RemoteCloseSCRAMServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseSCRAMNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseSCRAMServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Starts a SASL SCRAM-SHA-256 exchange, then closes after the client's
  SASLInitialResponse arrives (session state: _SessionSCRAMAuthenticating).
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
        _tcp_connection.close()
      end
    end

class \nodoc\ iso _TestRemoteCloseLoggedInIdle is UnitTest
  """
  Peer close during `_SessionLoggedIn` with no in-flight query. Asserts
  `pg_session_shutdown` and no `pg_query_failed`.
  """
  fun name(): String =>
    "RemoteClose/LoggedInIdle"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7748"

    let listener = _RemoteCloseLoggedInIdleListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseLoggedInIdleNotify is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(s: Session) =>
    _authenticated = true

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail(
        "pg_session_shutdown fired without reaching the logged-in state.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseLoggedInIdleListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseLoggedInIdleServer =>
    let server = _RemoteCloseLoggedInIdleServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseLoggedInIdleNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseLoggedInIdleServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, signals ReadyForQuery, then closes.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _closed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _closed then return end
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _closed = true
      _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
      _tcp_connection.send(_IncomingReadyForQueryTestMessage('I').bytes())
      _tcp_connection.close()
    end

class \nodoc\ iso _TestRemoteCloseLoggedInInFlight is UnitTest
  """
  Peer close during `_SessionLoggedIn` with an in-flight simple query.
  Asserts `pg_query_failed(SessionClosed)` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/LoggedInInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7749"

    let listener = _RemoteCloseLoggedInInFlightListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseLoggedInInFlightClient is
  (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query: SimpleQuery = SimpleQuery("SELECT 1")
  // Counts rather than booleans so a regression that fires the callback
  // twice (e.g., dropping the `_error = true` guard so both
  // `query_state.on_closed` and `drain_in_flight` deliver the failure)
  // is caught by the test.
  var _query_failed_count: USize = 0
  var _shutdown_count: USize = 0

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
    | SessionClosed => _query_failed_count = _query_failed_count + 1
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    _shutdown_count = _shutdown_count + 1
    if _query_failed_count != 1 then
      _h.fail("pg_query_failed fired " + _query_failed_count.string()
        + " times; expected exactly 1.")
      _h.complete(false)
      return
    end
    if _shutdown_count != 1 then
      _h.fail("pg_session_shutdown fired " + _shutdown_count.string()
        + " times; expected exactly 1.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseLoggedInInFlightListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseLoggedInInFlightClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseAfterQueryServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, waits for a message after ReadyForQuery,
  then closes — putting the client in `_SessionLoggedIn` with a query in
  flight.
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
        _tcp_connection.close()
      end
    end

class \nodoc\ iso _TestRemoteCloseLoggedInPipeline is UnitTest
  """
  Peer close during `_SessionLoggedIn` with an in-flight two-query
  pipeline. Asserts both queries get `pg_pipeline_failed(SessionClosed)`,
  then `pg_pipeline_complete`, then `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/LoggedInPipeline"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7750"

    let listener = _RemoteCloseLoggedInPipelineListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseLoggedInPipelineClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _got_index_0: Bool = false
  var _got_index_1: Bool = false
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
    | (0, SessionClosed) => _got_index_0 = true
    | (1, SessionClosed) => _got_index_1 = true
    else
      _h.fail(
        "Unexpected pipeline failure at index " + index.string() + ".")
      _h.complete(false)
    end

  be pg_pipeline_complete(session: Session) =>
    if not (_got_index_0 and _got_index_1) then
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

actor \nodoc\ _RemoteCloseLoggedInPipelineListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    // Reuse the post-ready close server: the client sends all pipeline
    // parts in a single write, so the server sees one message, then
    // closes.
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseLoggedInPipelineClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteCloseExtendedQueryInFlight is UnitTest
  """
  Peer close during an in-flight `PreparedQuery` execution (state
  `_ExtendedQueryInFlight`). The simple-query and extended-query states
  have duplicated notification logic — this test covers the extended side.
  """
  fun name(): String =>
    "RemoteClose/ExtendedQueryInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7751"

    let listener = _RemoteCloseExtendedQueryListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseExtendedQueryClient is
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
    | SessionClosed => _query_failed = true
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _query_failed then
      _h.fail("pg_session_shutdown fired before pg_query_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseExtendedQueryListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseExtendedQueryClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteClosePrepareInFlight is UnitTest
  """
  Peer close during an in-flight `prepare` (state `_PrepareInFlight`).
  Asserts `pg_prepare_failed(SessionClosed)` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/PrepareInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7752"

    let listener = _RemoteClosePrepareListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteClosePrepareClient is
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
    | SessionClosed => _prepare_failed = true
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _prepare_failed then
      _h.fail("pg_session_shutdown fired before pg_prepare_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteClosePrepareListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteClosePrepareClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteCloseCopyInInFlight is UnitTest
  """
  Peer close during an in-flight `copy_in` (state `_CopyInInFlight`).
  Asserts `pg_copy_failed(SessionClosed)` on the `CopyInReceiver` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/CopyInInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7753"

    let listener = _RemoteCloseCopyInListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseCopyInClient is
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
    | SessionClosed => _copy_failed = true
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _copy_failed then
      _h.fail("pg_session_shutdown fired before pg_copy_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseCopyInListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseCopyInClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteCloseCopyOutInFlight is UnitTest
  """
  Peer close during an in-flight `copy_out` (state `_CopyOutInFlight`).
  Asserts `pg_copy_failed(SessionClosed)` on the `CopyOutReceiver` before
  `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/CopyOutInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7754"

    let listener = _RemoteCloseCopyOutListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseCopyOutClient is
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
    | SessionClosed => _copy_failed = true
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _copy_failed then
      _h.fail("pg_session_shutdown fired before pg_copy_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseCopyOutListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseCopyOutClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteCloseStreamInFlight is UnitTest
  """
  Peer close during an in-flight `stream` (state `_StreamingQueryInFlight`).
  Asserts `pg_stream_failed(SessionClosed)` before `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/StreamInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7755"

    let listener = _RemoteCloseStreamListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseStreamClient is
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
    | SessionClosed => _stream_failed = true
    else
      _h.fail("Expected SessionClosed.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    if not _stream_failed then
      _h.fail("pg_session_shutdown fired before pg_stream_failed.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseStreamListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseStreamClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteClosePostAuthPreReady is UnitTest
  """
  Peer close after `AuthenticationOk` but before the first `ReadyForQuery`.
  The session is `_SessionLoggedIn` with `query_state = _QueryNotReady` —
  no query in flight, no ReadyForQuery observed. Asserts
  `pg_session_authenticated` fires, then `pg_session_shutdown` fires with
  no `pg_query_failed` in between (nothing was queued).
  """
  fun name(): String =>
    "RemoteClose/PostAuthPreReady"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7756"

    let listener = _RemoteClosePostAuthPreReadyListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteClosePostAuthPreReadyNotify is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unexpected pre-ready failure.")
    _h.complete(false)

  be pg_session_authenticated(s: Session) =>
    _authenticated = true

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail(
        "pg_session_shutdown fired without reaching the logged-in state.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteClosePostAuthPreReadyListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteClosePostAuthPreReadyServer =>
    let server = _RemoteClosePostAuthPreReadyServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteClosePostAuthPreReadyNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteClosePostAuthPreReadyServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Reads the client's StartupMessage, sends `AuthenticationOk` (no
  `ReadyForQuery`), then closes. Leaves the client in `_SessionLoggedIn`
  with `query_state = _QueryNotReady`.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _closed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _closed then return end
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      _closed = true
      _tcp_connection.send(_IncomingAuthenticationOkTestMessage.bytes())
      _tcp_connection.close()
    end

class \nodoc\ iso _TestRemoteCloseCloseStatementInFlight is UnitTest
  """
  Peer close during an in-flight `close_statement` (state
  `_CloseStatementInFlight`). `close_statement` is fire-and-forget with
  no receiver — the observable is a clean `pg_session_shutdown` without
  any crash. Mirrors the protocol-violation coverage pattern.
  """
  fun name(): String =>
    "RemoteClose/CloseStatementInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7757"

    let listener = _RemoteCloseCloseStatementListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseCloseStatementClient is SessionStatusNotify
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

actor \nodoc\ _RemoteCloseCloseStatementListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterQueryServer =>
    let server = _RemoteCloseAfterQueryServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseCloseStatementClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestRemoteCloseAfterErrorResponse is UnitTest
  """
  Server sends `ErrorResponse` then closes the TCP connection before
  `ReadyForQuery`. `on_error_response` already sets the in-flight state's
  `_error = true` and delivers `pg_query_failed(ErrorResponseMessage)`, so
  the subsequent `on_closed` must NOT double-deliver the query failure.
  Uses counters to assert exactly-once delivery on both `pg_query_failed`
  and `pg_session_shutdown`.
  """
  fun name(): String =>
    "RemoteClose/AfterErrorResponse"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7758"

    let listener = _RemoteCloseAfterErrorResponseListener(
      lori.TCPListenAuth(h.env.root), host, port, h)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseAfterErrorResponseClient is
  (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query: SimpleQuery = SimpleQuery("SELECT 1")
  var _query_failed_count: USize = 0
  var _got_error_response: Bool = false
  var _shutdown_count: USize = 0

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
    _query_failed_count = _query_failed_count + 1
    match failure
    | let _: ErrorResponseMessage => _got_error_response = true
    else
      _h.fail("Expected ErrorResponseMessage on first call.")
      _h.complete(false)
    end

  be pg_session_shutdown(s: Session) =>
    _shutdown_count = _shutdown_count + 1
    if not _got_error_response then
      _h.fail("pg_session_shutdown fired before pg_query_failed.")
      _h.complete(false)
      return
    end
    if _query_failed_count != 1 then
      _h.fail("pg_query_failed fired " + _query_failed_count.string()
        + " times; expected exactly 1.")
      _h.complete(false)
      return
    end
    if _shutdown_count != 1 then
      _h.fail("pg_session_shutdown fired " + _shutdown_count.string()
        + " times; expected exactly 1.")
      _h.complete(false)
      return
    end
    _h.complete(true)

actor \nodoc\ _RemoteCloseAfterErrorResponseListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _RemoteCloseAfterErrorResponseServer =>
    let server = _RemoteCloseAfterErrorResponseServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseAfterErrorResponseClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _RemoteCloseAfterErrorResponseServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Authenticates the client, waits for a query, then responds with
  `ErrorResponse` and closes WITHOUT sending `ReadyForQuery`. Forces the
  `on_closed`/`drain_in_flight` double-delivery risk.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader
  var _authed: Bool = false
  var _closed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _closed then return end
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
        _closed = true
        _tcp_connection.send(
          _IncomingErrorResponseTestMessage(
            "ERROR", "42601", "syntax error").bytes())
        _tcp_connection.close()
      end
    end

class \nodoc\ iso _TestRemoteCloseSSLNegotiatingPreferred is UnitTest
  """
  Peer close during `_SessionSSLNegotiating` when the client is using
  `SSLPreferred`. The client still fires `ConnectionClosedByServer`
  (plaintext fallback applies only to an explicit 'N' response, not to a
  dead connection). Sibling to `RemoteClose/SSLNegotiating` which covers
  the `SSLRequired` path.
  """
  fun name(): String =>
    "RemoteClose/SSLNegotiatingPreferred"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7759"

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _RemoteCloseSSLNegotiatingPreferredListener(
      lori.TCPListenAuth(h.env.root), host, port, h, sslctx)
    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _RemoteCloseSSLNegotiatingPreferredListener
  is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _sslctx: SSLContext val

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper, sslctx: SSLContext val)
  =>
    _host = host
    _port = port
    _h = h
    _sslctx = sslctx
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _RemoteCloseAfterSSLRequestServer =>
    let server = _RemoteCloseAfterSSLRequestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLPreferred(_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _RemoteCloseSSLNegotiatingNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)
