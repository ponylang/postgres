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

// SSLPreferred unit tests

class \nodoc\ iso _TestSSLPreferredFallback is UnitTest
  """
  Verifies that when using SSLPreferred and the server responds 'N' to an
  SSLRequest, the session falls back to plaintext and successfully connects
  and authenticates.
  """
  fun name(): String =>
    "SSLPreferred/Fallback"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7707"

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let listener = _SSLPreferredFallbackTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLPreferredFallbackTestNotify is SessionStatusNotify
  let _h: TestHelper
  var _connected: Bool = false
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connected(s: Session) =>
    _connected = true

  be pg_session_authenticated(session: Session) =>
    _authenticated = true
    session.close()
    if _connected then
      _h.complete(true)
    else
      _h.fail("Authenticated but never connected")
      _h.complete(false)
    end

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Should not have gotten connection_failed with SSLPreferred")
    _h.complete(false)

  be pg_session_shutdown(s: Session) =>
    if not _authenticated then
      _h.fail("Unexpected shutdown before authentication")
      _h.complete(false)
    end

actor \nodoc\ _SSLPreferredFallbackTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _SSLPreferredFallbackTestServer =>
    _SSLPreferredFallbackTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLPreferred(_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLPreferredFallbackTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLPreferredFallbackTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that responds 'N' to an SSLRequest (refusing SSL), then reads
  the plaintext StartupMessage and sends AuthOk + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _ssl_refused: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _ssl_refused then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        // Client sent SSLRequest — respond 'N' (refuse SSL)
        let response: Array[U8] val = ['N']
        _tcp_connection.send(response)
        _ssl_refused = true
        _process()
      end
    else
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        // StartupMessage received over plaintext — send AuthOk + ReadyForQuery
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
      end
    end

class \nodoc\ iso _TestSSLPreferredSuccess is UnitTest
  """
  Verifies that when using SSLPreferred and the server responds 'S' to an
  SSLRequest, the TLS handshake completes and the session connects and
  authenticates over SSL — same as SSLRequired when the server accepts.
  """
  fun name(): String =>
    "SSLPreferred/Success"

  fun apply(h: TestHelper) ? =>
    let host = "127.0.0.1"
    let port = "7708"

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

    let listener = _SSLPreferredSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      client_sslctx,
      server_sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLPreferredSuccessTestNotify is SessionStatusNotify
  let _h: TestHelper
  var _authenticated: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connected(s: Session) =>
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

actor \nodoc\ _SSLPreferredSuccessTestListener is lori.TCPListenerActor
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
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLPreferred(_client_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLPreferredSuccessTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestSSLPreferredTLSFailure is UnitTest
  """
  Verifies that when using SSLPreferred and the server responds 'S' but uses
  an incompatible TLS configuration causing the handshake to fail, the
  session fires pg_session_connection_failed — NOT a fallback to plaintext.
  TLS handshake failure is a hard failure regardless of SSL mode.
  """
  fun name(): String =>
    "SSLPreferred/TLSFailure"

  fun apply(h: TestHelper) ? =>
    let host = "127.0.0.1"
    let port = "7709"

    // Client requires TLS 1.3 minimum, server only offers TLS 1.2 max.
    // This creates an incompatible TLS configuration that will cause the
    // handshake to fail.
    let client_sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
        .> set_min_proto_version(Tls1u3Version())?
    end

    let cert_path = FilePath(FileAuth(h.env.root),
      "assets/test-cert.pem")
    let key_path = FilePath(FileAuth(h.env.root),
      "assets/test-key.pem")

    let server_sslctx = recover val
      SSLContext
        .> set_cert(cert_path, key_path)?
        .> set_client_verify(false)
        .> set_server_verify(false)
        .> set_max_proto_version(Tls1u2Version())?
    end

    let listener = _SSLPreferredTLSFailureTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      client_sslctx,
      server_sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLPreferredTLSFailureTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.complete(true)

  be pg_session_connected(s: Session) =>
    _h.fail("Should not have connected after TLS failure")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated after TLS failure")
    _h.complete(false)

actor \nodoc\ _SSLPreferredTLSFailureTestListener is lori.TCPListenerActor
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
    // Reuses _SSLSuccessTestServer — it responds 'S' and attempts TLS.
    // The incompatible TLS configs cause the handshake to fail.
    _SSLSuccessTestServer(_server_auth, _server_sslctx, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLPreferred(_client_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SSLPreferredTLSFailureTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestSSLPreferredCancelFallback is UnitTest
  """
  Verifies that when using SSLPreferred and the cancel connection's server
  refuses SSL ('N'), the _CancelSender falls back to plaintext and sends a
  valid CancelRequest. The main session uses SSLPreferred with a server that
  accepts SSL, but the cancel connection encounters a refusal.
  """
  fun name(): String =>
    "SSLPreferred/CancelFallback"

  fun apply(h: TestHelper) ? =>
    let host = "127.0.0.1"
    let port = "7710"

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

    let listener = _SSLPreferredCancelTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      client_sslctx,
      server_sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLPreferredCancelTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _client_sslctx: SSLContext val
  let _server_sslctx: SSLContext val
  var _connection_count: USize = 0

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

  fun ref _on_accept(fd: U32): _SSLPreferredCancelTestServer =>
    _connection_count = _connection_count + 1
    _SSLPreferredCancelTestServer(_server_auth, _server_sslctx, fd, _h,
      _connection_count > 1)

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port,
        SSLPreferred(_client_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CancelTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLPreferredCancelTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that handles two connections: the first (main session) accepts
  SSL and authenticates; the second (cancel sender) refuses SSL ('N') so the
  cancel falls back to plaintext, then verifies the CancelRequest.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _sslctx: SSLContext val
  let _h: TestHelper
  let _is_cancel_connection: Bool
  var _ssl_started: Bool = false
  var _ssl_refused: Bool = false
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, sslctx: SSLContext val, fd: U32,
    h: TestHelper, is_cancel: Bool)
  =>
    _sslctx = sslctx
    _h = h
    _is_cancel_connection = is_cancel
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _is_cancel_connection then
      if not _ssl_refused then
        match _reader.read_startup_message()
        | let _: Array[U8] val =>
          // Cancel connection: refuse SSL so _CancelSender falls back
          let response: Array[U8] val = ['N']
          _tcp_connection.send(response)
          _ssl_refused = true
          _process()
        end
      else
        match _reader.read_startup_message()
        | let msg: Array[U8] val =>
          // Verify CancelRequest: 16 bytes total
          // Int32(16) Int32(80877102) Int32(pid=12345) Int32(key=67890)
          if msg.size() != 16 then
            _h.fail("CancelRequest should be 16 bytes, got "
              + msg.size().string())
            _h.complete(false)
            return
          end

          try
            if (msg(0)? != 0) or (msg(1)? != 0) or (msg(2)? != 0)
              or (msg(3)? != 16) then
              _h.fail("CancelRequest length field is incorrect")
              _h.complete(false)
              return
            end

            if (msg(4)? != 4) or (msg(5)? != 210) or (msg(6)? != 22)
              or (msg(7)? != 46) then
              _h.fail("CancelRequest magic number is incorrect")
              _h.complete(false)
              return
            end

            if (msg(8)? != 0) or (msg(9)? != 0) or (msg(10)? != 48)
              or (msg(11)? != 57) then
              _h.fail("CancelRequest process_id is incorrect")
              _h.complete(false)
              return
            end

            if (msg(12)? != 0) or (msg(13)? != 1) or (msg(14)? != 9)
              or (msg(15)? != 50) then
              _h.fail("CancelRequest secret_key is incorrect")
              _h.complete(false)
              return
            end

            _h.complete(true)
          else
            _h.fail("Error reading CancelRequest bytes")
            _h.complete(false)
          end
        end
      end
    else
      // Main session connection
      if not _ssl_started then
        match _reader.read_startup_message()
        | let _: Array[U8] val =>
          // SSLRequest — respond 'S' and upgrade to TLS
          let response: Array[U8] val = ['S']
          _tcp_connection.send(response)
          match _tcp_connection.start_tls(_sslctx)
          | None => _ssl_started = true
          | let _: lori.StartTLSError =>
            _tcp_connection.close()
          end
        end
      elseif not _authed then
        match _reader.read_startup_message()
        | let _: Array[U8] val =>
          _authed = true
          let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
          let bkd = _IncomingBackendKeyDataTestMessage(12345, 67890).bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          let combined: Array[U8] val = recover val
            let arr = Array[U8]
            arr.append(auth_ok)
            arr.append(bkd)
            arr.append(ready)
            arr
          end
          _tcp_connection.send(combined)
        end
      end
      // After auth, receive query data and hold (don't respond)
    end

// SSLPreferred integration tests

class \nodoc\ iso _TestSSLPreferredWithSSLServer is UnitTest
  """
  Verifies that connecting with SSLPreferred to an SSL-enabled PostgreSQL
  server results in a successful SSL connection and authentication.
  """
  fun name(): String =>
    "integration/SSLPreferred/WithSSLServer"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host,
        info.ssl_port, SSLPreferred(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _AuthenticateTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSSLPreferredWithPlainServer is UnitTest
  """
  Verifies that connecting with SSLPreferred to a PostgreSQL server that
  does not support SSL results in a successful plaintext fallback connection
  and authentication.
  """
  fun name(): String =>
    "integration/SSLPreferred/WithPlainServer"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host,
        info.port, SSLPreferred(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _AuthenticateTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)
