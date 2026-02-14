use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestParameterStatusDelivery is UnitTest
  """
  Verifies that pg_parameter_status fires with the correct name and value
  when the server sends a ParameterStatus message between CommandComplete
  and ReadyForQuery.
  """
  fun name(): String =>
    "ParameterStatus/Delivery"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7696"

    let listener = _ParameterStatusTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _ParameterStatusDeliveryClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _ParameterStatusDeliveryClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_result: Bool = false
  var _got_status: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    _got_status = true
    if status.name != "application_name" then
      _h.fail("Expected name 'application_name' but got '" +
        status.name + "'")
      session.close()
      _h.complete(false)
      return
    end
    if status.value != "test_app" then
      _h.fail("Expected value 'test_app' but got '" +
        status.value + "'")
      session.close()
      _h.complete(false)
      return
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and status.
    if _got_result then
      if not _got_status then
        _h.fail("Got query result but no parameter status.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

class \nodoc\ iso _TestParameterStatusDuringDataRows is UnitTest
  """
  Verifies that a ParameterStatus arriving between DataRow messages
  still delivers both the complete query result (with all data rows) and the
  parameter status.
  """
  fun name(): String =>
    "ParameterStatus/DuringDataRows"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7697"

    let listener = _ParameterStatusMidQueryTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _ParameterStatusDuringDataRowsClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _ParameterStatusDuringDataRowsClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_status: Bool = false
  var _got_result: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true
    match result
    | let r: ResultSet =>
      if r.rows().size() != 2 then
        _h.fail("Expected 2 rows but got " + r.rows().size().string())
        session.close()
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet.")
      session.close()
      _h.complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    _got_status = true
    if status.name != "application_name" then
      _h.fail("Expected name 'application_name' but got '" +
        status.name + "'")
      session.close()
      _h.complete(false)
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and status.
    if _got_result then
      if not _got_status then
        _h.fail("Got query result but no parameter status.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

// Shared mock server infrastructure for parameter status tests

actor \nodoc\ _ParameterStatusTestListener is lori.TCPListenerActor
  """
  Mock server that authenticates, waits for a query, then responds with
  CommandComplete + ParameterStatus + ReadyForQuery.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _notify: (SessionStatusNotify & ResultReceiver)

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    notify: (SessionStatusNotify & ResultReceiver),
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _notify = notify
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _ParameterStatusTestServer =>
    _ParameterStatusTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _ParameterStatusTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  CommandComplete + ParameterStatus + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
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
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _process()
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        let cmd = _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
        let ps = _IncomingParameterStatusTestMessage(
          "application_name", "test_app").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(cmd)
        _tcp_connection.send(ps)
        _tcp_connection.send(ready)
      end
    end

actor \nodoc\ _ParameterStatusMidQueryTestListener is lori.TCPListenerActor
  """
  Mock server listener for the mid-query parameter status test.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _notify: (SessionStatusNotify & ResultReceiver)

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    notify: (SessionStatusNotify & ResultReceiver),
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _notify = notify
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _ParameterStatusMidQueryTestServer =>
    _ParameterStatusMidQueryTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _ParameterStatusMidQueryTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  RowDescription + DataRow + ParameterStatus + DataRow +
  CommandComplete + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
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
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _process()
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        try
          let columns: Array[(String, String)] val = [("col", "text")]
          let row_desc =
            _IncomingRowDescriptionTestMessage(columns)?.bytes()

          let row1_cols: Array[(String | None)] val = ["row1"]
          let data_row_1 = _IncomingDataRowTestMessage(row1_cols).bytes()
          let row2_cols: Array[(String | None)] val = ["row2"]
          let data_row_2 = _IncomingDataRowTestMessage(row2_cols).bytes()

          let ps = _IncomingParameterStatusTestMessage(
            "application_name", "test_app").bytes()

          let cmd = _IncomingCommandCompleteTestMessage("SELECT 2").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()

          _tcp_connection.send(row_desc)
          _tcp_connection.send(data_row_1)
          _tcp_connection.send(ps)
          _tcp_connection.send(data_row_2)
          _tcp_connection.send(cmd)
          _tcp_connection.send(ready)
        end
      end
    end

// Integration tests

class \nodoc\ iso _TestParameterStatusOnStartup is UnitTest
  """
  Verifies that pg_parameter_status fires during startup with at least the
  server_version parameter when connecting to a real PostgreSQL server.
  """
  fun name(): String =>
    "integration/ParameterStatus/Startup"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _ParameterStatusStartupClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _ParameterStatusStartupClient is SessionStatusNotify
  let _h: TestHelper
  let _session: Session
  var _got_server_version: Bool = false

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    None

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    if status.name == "server_version" then
      _got_server_version = true
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // ReadyForQuery fires after all startup ParameterStatus messages.
    if not _got_server_version then
      _h.fail("No server_version ParameterStatus received during startup.")
      _session.close()
      _h.complete(false)
      return
    end
    _session.close()
    _h.complete(true)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestParameterStatusOnSet is UnitTest
  """
  Verifies that pg_parameter_status fires when a SET command changes a
  reporting parameter on a real PostgreSQL server.
  """
  fun name(): String =>
    "integration/ParameterStatus/Set"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _ParameterStatusSetClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _ParameterStatusSetClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _session: Session
  var _got_result: Bool = false
  var _got_app_name: Bool = false

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.execute(
      SimpleQuery("SET application_name = 'pony_test_app'"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    if (status.name == "application_name")
      and (status.value == "pony_test_app")
    then
      _got_app_name = true
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    if _got_result then
      if not _got_app_name then
        _h.fail(
          "SET completed but no application_name ParameterStatus received.")
        _session.close()
        _h.complete(false)
        return
      end
      _session.close()
      _h.complete(true)
    end

  be dispose() =>
    _session.close()
