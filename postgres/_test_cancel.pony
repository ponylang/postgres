use "files"
use lori = "lori"
use "pony_test"
use "ssl/net"

class \nodoc\ iso _TestCancelQueryInFlight is UnitTest
  """
  Verifies that calling cancel() when a query is in flight opens a separate
  TCP connection and sends a valid CancelRequest message containing the
  correct process ID and secret key from BackendKeyData.
  """
  fun name(): String =>
    "CancelQueryInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7675"

    let listener = _CancelTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CancelTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT pg_sleep(100)"), this)
    session.cancel()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    None

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    None

actor \nodoc\ _CancelTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  var _connection_count: USize = 0

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _CancelTestServer =>
    _connection_count = _connection_count + 1
    _CancelTestServer(_server_auth, fd, _h, _connection_count > 1)

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CancelTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CancelTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that handles two connections: the first is the main session
  (authenticates and becomes ready), the second is the cancel sender
  (verifies CancelRequest format and content).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  let _is_cancel_connection: Bool
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    is_cancel: Bool)
  =>
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
    else
      if not _authed then
        match _reader.read_startup_message()
        | let _: Array[U8] val =>
          _authed = true
          let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
          let bkd = _IncomingBackendKeyDataTestMessage(12345, 67890).bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          // Send all auth messages in a single write so the Session
          // processes them atomically. If sent separately, TCP may deliver
          // them in different reads, causing ReadyForQuery to arrive after
          // the client has already called cancel() (which would see
          // _QueryNotReady).
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

// SSL cancel query unit test

class \nodoc\ iso _TestSSLCancelQueryInFlight is UnitTest
  """
  Verifies that calling cancel() on an SSL session opens a separate
  SSL-negotiated TCP connection and sends a valid CancelRequest message
  containing the correct process ID and secret key.
  """
  fun name(): String =>
    "SSLCancelQueryInFlight"

  fun apply(h: TestHelper) ? =>
    let host = "127.0.0.1"
    let port = "7676"

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

    let listener = _SSLCancelTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      client_sslctx,
      server_sslctx)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SSLCancelTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _SSLCancelTestServer =>
    _connection_count = _connection_count + 1
    _SSLCancelTestServer(_server_auth, _server_sslctx, fd, _h,
      _connection_count > 1)

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port, SSLRequired(_client_sslctx)),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CancelTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SSLCancelTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock SSL server that handles two connections: the first is the main session
  (SSL negotiation + authenticate + ready), the second is the cancel sender
  (SSL negotiation + verify CancelRequest format and content).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _sslctx: SSLContext val
  let _h: TestHelper
  let _is_cancel_connection: Bool
  var _ssl_started: Bool = false
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
    elseif _is_cancel_connection then
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
    else
      if not _authed then
        match _reader.read_startup_message()
        | let _: Array[U8] val =>
          _authed = true
          let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
          let bkd = _IncomingBackendKeyDataTestMessage(12345, 67890).bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          // Send all auth messages in a single write so the Session
          // processes them atomically (same reason as _CancelTestServer).
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

// Cancel integration tests

class \nodoc\ iso _TestCancelPgSleep is UnitTest
  """
  Verifies that cancelling a long-running query on a real PostgreSQL server
  produces a query failure with SQLSTATE 57014 (query_canceled).
  """
  fun name(): String =>
    "integration/Cancel/Query"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _CancelPgSleepClient(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _CancelPgSleepClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT pg_sleep(30)")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)
    session.cancel()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected query to be cancelled, but got a result.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if query isnt _query then
      _h.fail("Got failure for unexpected query.")
      _h.complete(false)
      return
    end

    match failure
    | let err: ErrorResponseMessage =>
      if err.code == "57014" then
        _h.complete(true)
      else
        _h.fail("Expected SQLSTATE 57014 but got " + err.code)
        _h.complete(false)
      end
    | let ce: ClientQueryError =>
      _h.fail("Expected ErrorResponseMessage but got ClientQueryError.")
      _h.complete(false)
    end

class \nodoc\ iso _TestCancelSSLPgSleep is UnitTest
  """
  Verifies that cancelling a long-running query on a real PostgreSQL server
  over an SSL-encrypted connection produces a query failure with SQLSTATE
  57014 (query_canceled).
  """
  fun name(): String =>
    "integration/SSL/Cancel"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let client = _CancelPgSleepClient(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)
