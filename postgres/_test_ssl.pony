use "files"
use lori = "lori"
use "pony_test"
use "ssl/net"

// SSL negotiation unit tests

class \nodoc\ iso _TestSSLNegotiationRefused is UnitTest
  """
  Verifies that when the server responds 'N' to an SSLRequest, the session
  fires pg_session_connection_failed and shuts down.
  """
  fun name(): String =>
    "SSLNegotiation/Refused"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7671"

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _SSLRefusedTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLRefusedTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.complete(true)

  be pg_session_connected(s: Session) =>
    _h.fail("Should not have connected")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    _h.fail("Should not have gotten shutdown")
    _h.complete(false)

actor \nodoc\ _SSLRefusedTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _sslctx: SSLContext val

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    sslctx: SSLContext val)
  =>
    _host = host
    _port = port
    _h = h
    _sslctx = sslctx
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SSLRefusedTestServer =>
    _SSLRefusedTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port, SSLRequired(_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLRefusedTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLRefusedTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that responds 'N' to an SSLRequest, refusing SSL.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let response: Array[U8] val = ['N']
    _tcp_connection.send(response)

class \nodoc\ iso _TestSSLNegotiationJunkResponse is UnitTest
  """
  Verifies that when the server responds with a junk byte (not 'S' or 'N')
  to an SSLRequest, the session shuts down.
  """
  fun name(): String =>
    "SSLNegotiation/JunkResponse"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7672"

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _SSLJunkTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLJunkTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_shutdown(s: Session) =>
    _h.complete(true)

  be pg_session_connected(s: Session) =>
    _h.fail("Should not have connected")
    _h.complete(false)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Should not have gotten connection_failed for junk")
    _h.complete(false)

actor \nodoc\ _SSLJunkTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _sslctx: SSLContext val

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    sslctx: SSLContext val)
  =>
    _host = host
    _port = port
    _h = h
    _sslctx = sslctx
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SSLJunkTestServer =>
    _SSLJunkTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port, SSLRequired(_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLJunkTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLJunkTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that responds with a junk byte to an SSLRequest.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let response: Array[U8] val = ['X']
    _tcp_connection.send(response)

class \nodoc\ iso _TestSSLNegotiationSuccess is UnitTest
  """
  Verifies the full SSL negotiation happy path: server responds 'S', TLS
  handshake completes, StartupMessage is sent over the encrypted connection,
  and the session fires pg_session_connected then pg_session_authenticated.
  """
  fun name(): String =>
    "SSLNegotiation/Success"

  fun apply(h: TestHelper) ? =>
    let host = "127.0.0.1"
    let port = "7673"

    let cert_path = FilePath(FileAuth(h.env.root),
      "assets/test-cert.pem")
    let key_path = FilePath(FileAuth(h.env.root),
      "assets/test-key.pem")

    let client_sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let server_sslctx = recover val
      SSLContext
        .> set_cert(cert_path, key_path)?
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _SSLSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      client_sslctx,
      server_sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLSuccessTestNotify is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connected(s: Session) =>
    // TLS handshake completed and session is ready for authentication
    None

  be pg_session_authenticated(session: Session) =>
    _authenticated = true
    session.close()
    _h.complete(true)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Connection failed during SSL negotiation")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail("Unexpected shutdown before authentication")
      _h.complete(false)
    end

actor \nodoc\ _SSLSuccessTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _client_sslctx: SSLContext val
  let _server_sslctx: SSLContext val

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    client_sslctx: SSLContext val,
    server_sslctx: SSLContext val)
  =>
    _host = host
    _port = port
    _h = h
    _client_sslctx = client_sslctx
    _server_sslctx = server_sslctx
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SSLSuccessTestServer =>
    _SSLSuccessTestServer(_server_auth, _server_sslctx, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port, SSLRequired(_client_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLSuccessTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLSuccessTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that responds 'S' to an SSLRequest, upgrades to TLS on its
  side, then sends AuthenticationOk + ReadyForQuery over the encrypted
  connection once it receives the StartupMessage.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _sslctx: SSLContext val
  var _ssl_started: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, sslctx: SSLContext val, fd: U32) =>
    _sslctx = sslctx
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _ssl_started then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        // Client sent SSLRequest — respond 'S' and upgrade to TLS
        let response: Array[U8] val = ['S']
        _tcp_connection.send(response)
        match _tcp_connection.start_tls(_sslctx)
        | None => _ssl_started = true
        | let _: lori.StartTLSError =>
          _tcp_connection.close()
        end
      end
    else
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        // StartupMessage received over TLS — send AuthOk + ReadyForQuery
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
      end
    end

// SSL integration tests

class \nodoc\ iso _TestSSLConnect is UnitTest
  """
  Verifies that connecting with SSLRequired to a PostgreSQL server with SSL
  enabled results in a successful connection.
  """
  fun name(): String =>
    "integration/SSL/Connect"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSSLAuthenticate is UnitTest
  """
  Verifies that connecting with SSLRequired to a PostgreSQL server with SSL
  enabled allows successful authentication over the encrypted connection.
  """
  fun name(): String =>
    "integration/SSL/Authenticate"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _AuthenticateTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSSLQueryResults is UnitTest
  """
  Verifies that queries can be executed and results received over an
  SSL-encrypted connection.
  """
  fun name(): String =>
    "integration/SSL/Query"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let client = _ResultsIncludeOriginatingQueryReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSSLRefused is UnitTest
  """
  Verifies that connecting with SSLRequired to a PostgreSQL server that does
  not support SSL results in pg_session_connection_failed. Unlike the
  SSLNegotiation/Refused unit test which uses a mock server, this tests
  against a real PostgreSQL instance.
  """
  fun name(): String =>
    "integration/SSL/Refused"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, false))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)
