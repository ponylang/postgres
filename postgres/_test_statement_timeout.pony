use "constrained_types"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestStatementTimeoutFires is UnitTest
  """
  Verifies that when a query exceeds its statement_timeout, the driver
  sends a CancelRequest on a separate TCP connection. The mock server
  authenticates the session, receives a query, and holds (doesn't respond).
  The timer fires and a CancelRequest arrives on the second connection.
  """
  fun name(): String =>
    "StatementTimeout/Fires"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7720"

    let listener = _TimeoutTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TimeoutTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    // Execute a query with a 100ms timeout. The mock server will hold
    // (not respond), so the timer will fire and send a CancelRequest.
    match lori.MakeTimerDuration(100)
    | let d: lori.TimerDuration =>
      session.execute(SimpleQuery("SELECT pg_sleep(100)"), this
        where statement_timeout = d)
    | let _: ValidationFailure =>
      _h.fail("Failed to create TimerDuration.")
      _h.complete(false)
    end

  be pg_query_result(session: Session, result: Result) =>
    None

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    None

actor \nodoc\ _TimeoutTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _TimeoutTestServer =>
    _connection_count = _connection_count + 1
    let server = _TimeoutTestServer(_server_auth, fd, _h,
      _connection_count > 1)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _TimeoutTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _TimeoutTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that handles two connections: the first is the main session
  (authenticates, receives query, and holds without responding), the second
  is the cancel sender (verifies CancelRequest format and content).
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
      // After auth, receive query data and hold (don't respond) —
      // the statement timeout timer will fire.
    end

class \nodoc\ iso _TestStatementTimeoutRearmOnTimerFailure is UnitTest
  """
  Verifies that when the statement timer's ASIO event subscription fails,
  the driver rearms the timer with the in-flight operation's original
  duration instead of silently dropping the timeout. The client simulates
  the failure via `_test_trigger_on_timer_failure` immediately after
  dispatching a query; the rearmed timer fires and a CancelRequest arrives
  on the second connection.
  """
  fun name(): String =>
    "StatementTimeout/RearmOnTimerFailure"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7760"

    let listener = _TimeoutRearmTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TimeoutRearmTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    // Execute a query with a 200ms timeout, then immediately simulate an
    // ASIO subscription failure on the statement timer. The session
    // processes `execute` first (arms the original timer), then the
    // simulation (cancels the original token and rearms with 200ms). The
    // rearmed timer fires and sends a CancelRequest on a second connection.
    match lori.MakeTimerDuration(200)
    | let d: lori.TimerDuration =>
      session.execute(SimpleQuery("SELECT pg_sleep(100)"), this
        where statement_timeout = d)
      session._test_trigger_on_timer_failure()
    | let _: ValidationFailure =>
      _h.fail("Failed to create TimerDuration.")
      _h.complete(false)
    end

  be pg_query_result(session: Session, result: Result) =>
    None

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    None

actor \nodoc\ _TimeoutRearmTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _TimeoutTestServer =>
    _connection_count = _connection_count + 1
    let server = _TimeoutTestServer(_server_auth, fd, _h,
      _connection_count > 1)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _TimeoutRearmTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestStatementTimeoutCancelledOnCompletion is UnitTest
  """
  Verifies that when a query completes before its statement_timeout, the
  timer is cancelled and the result is delivered normally. The mock server
  authenticates and immediately responds with a successful query result.
  """
  fun name(): String =>
    "StatementTimeout/CancelledOnCompletion"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7721"

    let listener = _TimeoutCancelledTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TimeoutCancelledTestClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    // Execute with a long timeout (5s). The mock server responds immediately,
    // so the timer should be cancelled and pg_query_result should fire.
    match lori.MakeTimerDuration(5000)
    | let d: lori.TimerDuration =>
      session.execute(SimpleQuery("SELECT 1"), this
        where statement_timeout = d)
    | let _: ValidationFailure =>
      _h.fail("Failed to create TimerDuration.")
      _h.complete(false)
    end

  be pg_query_result(session: Session, result: Result) =>
    _h.complete(true)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query should have completed successfully.")
    _h.complete(false)

actor \nodoc\ _TimeoutCancelledTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

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

  fun ref _on_accept(fd: U32): _TimeoutCancelledTestServer =>
    let server = _TimeoutCancelledTestServer(_server_auth, fd, _h)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _TimeoutCancelledTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _TimeoutCancelledTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates the session and immediately responds to the
  query with a successful result, before the statement timeout can fire.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _authed then
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
    else
      // Read the query message from the client
      match _reader.read_message()
      | let _: Array[U8] val =>
        // Respond immediately with CommandComplete + ReadyForQuery
        let cc = _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        let combined: Array[U8] val = recover val
          let arr = Array[U8]
          arr.append(cc)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
      end
    end

// Integration tests

class \nodoc\ iso _TestStatementTimeoutPgSleep is UnitTest
  """
  Verifies that executing a long-running query with a short statement_timeout
  on a real PostgreSQL server produces a query failure with SQLSTATE 57014
  (query_canceled).
  """
  fun name(): String =>
    "integration/StatementTimeout/Query"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _TimeoutPgSleepClient(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _TimeoutPgSleepClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT pg_sleep(30)")

  be pg_session_authenticated(session: Session) =>
    match lori.MakeTimerDuration(1000)
    | let d: lori.TimerDuration =>
      session.execute(_query, this where statement_timeout = d)
    | let _: ValidationFailure =>
      _h.fail("Failed to create TimerDuration.")
      _h.complete(false)
    end

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Connection failed before reaching authenticated state.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected query to be cancelled by timeout, but got a result.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if query isnt _query then
      _h.fail("Got failure for unexpected query.")
      _h.complete(false)
      return
    end

    match \exhaustive\ failure
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
