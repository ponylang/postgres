use "collections"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestHandlingJunkMessages is UnitTest
  """
  Verifies that a session shuts down when receiving junk from the server.
  """
  fun name(): String =>
    "HandlingJunkMessages"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7669"

    let listener = _JunkSendingTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _HandlingJunkTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_shutdown(s: Session) =>
    _h.complete(true)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

actor \nodoc\ _JunkSendingTestListener is lori.TCPListenerActor
  """
  Listens for incoming connections and starts a server that will always reply
  with junk.
  """
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

  fun ref _on_accept(fd: U32): _JunkSendingTestServer =>
    _JunkSendingTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    // Now that we are listening, start a client session
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _HandlingJunkTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _JunkSendingTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Sends junk "postgres messages" in reponse to any incoming activity. This actor
  is used to test that our client handles getting junk correctly.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)
    let junk = _IncomingJunkTestMessage.bytes()
    _tcp_connection.send(junk)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let junk = _IncomingJunkTestMessage.bytes()
    _tcp_connection.send(junk)

class \nodoc\ iso _TestUnansweredQueriesFailOnShutdown is UnitTest
  """
  Verifies that when a session is shutting down, it sends "SessionClosed" query
  failures for any queries that are queued or haven't completed yet.

  Uses a misbehaving server (_DoesntAnswerTestServer) that authenticates but
  never sends ReadyForQuery, ensuring queries remain queued and never execute.
  When the client calls close(), the pending queries should all receive
  SessionClosed failures.
  """
  fun name(): String =>
    "UnansweredQueriesFailOnShutdown"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "9667"

    let listener = _DoesntAnswerTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _DoesntAnswerClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _in_flight_queries: SetIs[Query] = _in_flight_queries.create()

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _send_query(session, "select * from free_candy")
    _send_query(session, "select * from expensive_candy")
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Unexpectedly got a result for a query.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if _in_flight_queries.contains(query) then
      match failure
      | SessionClosed =>
        _in_flight_queries.unset(query)
        if _in_flight_queries.size() == 0 then
          _h.complete(true)
        end
      else
        _h.fail("Got an incorrect query failure reason.")
        _h.complete(false)
      end
    else
      _h.fail("Got a failure for a query we didn't send.")
      _h.complete(false)
    end

  fun ref _send_query(session: Session, string: String) =>
    let q = SimpleQuery(string)
    _in_flight_queries.set(q)
    session.execute(q, this)

actor \nodoc\ _DoesntAnswerTestListener is lori.TCPListenerActor
  """
  Listens for incoming connections and starts a server that will never reply
  """
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

  fun ref _on_accept(fd: U32): _DoesntAnswerTestServer =>
    _DoesntAnswerTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    // Now that we are listening, start a client session
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _DoesntAnswerClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _DoesntAnswerTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Simulates a misbehaving server that authenticates clients but never becomes
  ready for queries. It sends AuthenticationOk but intentionally omits the
  ReadyForQuery message, so the session transitions to _SessionLoggedIn with
  query_state stuck at _QueryNotReady. Any queued queries are never sent and
  remain pending until the client calls close(), at which point shutdown
  drains the queue and delivers SessionClosed failures to each receiver.
  """
  var _authed: Bool = false
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    """
    Sends AuthenticationOk on first contact without requiring a password.
    Intentionally does NOT send ReadyForQuery afterward — this is the
    misbehavior under test.
    """
    if not _authed then
      _authed = true
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      _tcp_connection.send(auth_ok)
    end

class \nodoc\ iso _TestZeroRowSelectReturnsResultSet is UnitTest
  """
  Verifies that a SELECT returning zero rows produces a ResultSet (not
  RowModifying). Uses a mock server that sends RowDescription followed by
  CommandComplete("SELECT 0") with no DataRow messages in between.
  """
  fun name(): String =>
    "ZeroRowSelectReturnsResultSet"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7670"

    let listener = _ZeroRowSelectTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _ZeroRowSelectTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query: SimpleQuery
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT * FROM empty_table")

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.execute(_query, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    if result.query() isnt _query then
      _h.fail("Query in result isn't the expected query.")
      _close_and_complete(false)
      return
    end

    match result
    | let r: ResultSet =>
      if r.rows().size() != 0 then
        _h.fail("Expected zero rows but got " + r.rows().size().string())
        _close_and_complete(false)
        return
      end
      if r.command() != "SELECT" then
        _h.fail("Expected command SELECT but got " + r.command())
        _close_and_complete(false)
        return
      end
    else
      _h.fail("Expected ResultSet but got a different result type.")
      _close_and_complete(false)
      return
    end

    _close_and_complete(true)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _ZeroRowSelectTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _ZeroRowSelectTestServer =>
    _ZeroRowSelectTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _ZeroRowSelectTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _ZeroRowSelectTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates and then responds to a query with
  RowDescription + CommandComplete("SELECT 0") — simulating a SELECT that
  returns zero rows.
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
          let columns: Array[(String, String)] val = recover val
            [("col", "text")]
          end
          let row_desc = _IncomingRowDescriptionTestMessage(columns)?.bytes()
          let cmd_complete =
            _IncomingCommandCompleteTestMessage("SELECT 0").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          _tcp_connection.send(row_desc)
          _tcp_connection.send(cmd_complete)
          _tcp_connection.send(ready)
        end
      end
    end

class \nodoc\ iso _TestPrepareShutdownDrainsPrepareQueue is UnitTest
  """
  Verifies that when a session shuts down, pending prepare() calls receive
  pg_prepare_failed with SessionClosed. Uses a misbehaving server that
  authenticates but never sends ReadyForQuery.
  """
  fun name(): String =>
    "PrepareShutdownDrainsPrepareQueue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "9668"

    let listener = _PrepareShutdownTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareShutdownTestClient is
  (SessionStatusNotify & PrepareReceiver)
  let _h: TestHelper
  var _pending: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _pending = 2
    session.prepare("s1", "SELECT 1", this)
    session.prepare("s2", "SELECT 2", this)
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _h.fail("Unexpectedly got a prepared statement.")
    _h.complete(false)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | SessionClosed =>
      _pending = _pending - 1
      if _pending == 0 then
        _h.complete(true)
      end
    else
      _h.fail("Got an incorrect prepare failure reason.")
      _h.complete(false)
    end

actor \nodoc\ _PrepareShutdownTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _DoesntAnswerTestServer =>
    _DoesntAnswerTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PrepareShutdownTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestTerminateSentOnClose is UnitTest
  """
  Verifies that closing a session sends a Terminate message to the server
  before closing the TCP connection. Uses a mock server that authenticates
  and becomes ready, then checks that the next data received from the client
  is a Terminate message ('X').
  """
  fun name(): String =>
    "TerminateSentOnClose"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7674"

    let listener = _TerminateSentTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TerminateSentTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.close()

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

actor \nodoc\ _TerminateSentTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _TerminateSentTestServer =>
    _TerminateSentTestServer(_server_auth, fd, _h)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _TerminateSentTestNotify(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _TerminateSentTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates clients and verifies that a Terminate
  message ('X') is received before the connection closes.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _h: TestHelper
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
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _process()
      end
    else
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'X' then
            _h.complete(true)
          end
        end
      end
    end

class \nodoc\ iso _TestByteaResultDecoding is UnitTest
  """
  Verifies that a bytea column (OID 17) is decoded from PostgreSQL's hex
  format into Array[U8] val. Uses a mock server that sends RowDescription
  with a bytea column followed by a DataRow containing hex-encoded bytes.
  """
  fun name(): String =>
    "Bytea/ResultDecoding"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7694"

    let listener = _ByteaTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      "\\x48656c6c6f",
      recover val [as U8: 72; 101; 108; 108; 111] end)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestEmptyByteaResultDecoding is UnitTest
  """
  Verifies that an empty bytea value (just the \\x prefix) is decoded into
  an empty Array[U8] val.
  """
  fun name(): String =>
    "Bytea/EmptyResultDecoding"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7695"

    let listener = _ByteaTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      "\\x",
      recover val Array[U8] end)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _ByteaTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _expected: Array[U8] val
  let _query: SimpleQuery
  var _session: (Session | None) = None

  new create(h: TestHelper, expected: Array[U8] val) =>
    _h = h
    _expected = expected
    _query = SimpleQuery("SELECT col")

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.execute(_query, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      try
        let field = r.rows()(0)?.fields(0)?
        match field.value
        | let actual: Array[U8] val =>
          _h.assert_array_eq[U8](_expected, actual)
        else
          _h.fail("Expected Array[U8] val but got a different type.")
        end
      else
        _h.fail("Expected at least one row with one field.")
      end
    else
      _h.fail("Expected ResultSet.")
    end
    _close_and_complete(true)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _ByteaTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _hex_data: String
  let _expected: Array[U8] val

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    hex_data: String,
    expected: Array[U8] val)
  =>
    _host = host
    _port = port
    _h = h
    _hex_data = hex_data
    _expected = expected
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _ByteaTestServer =>
    _ByteaTestServer(_server_auth, fd, _hex_data)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _ByteaTestClient(_h, _expected))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _ByteaTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates and responds to a query with
  RowDescription (one bytea column) + DataRow (hex-encoded value) +
  CommandComplete("SELECT 1") + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader
  let _hex_data: String

  new create(auth: lori.TCPServerAuth, fd: U32, hex_data: String) =>
    _hex_data = hex_data
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
          let columns: Array[(String, String)] val = recover val
            [("col", "bytea")]
          end
          let row_desc =
            _IncomingRowDescriptionTestMessage(columns)?.bytes()
          let data_row_cols: Array[(String | None)] val = recover val
            [_hex_data]
          end
          let data_row = _IncomingDataRowTestMessage(data_row_cols).bytes()
          let cmd_complete =
            _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          _tcp_connection.send(row_desc)
          _tcp_connection.send(data_row)
          _tcp_connection.send(cmd_complete)
          _tcp_connection.send(ready)
        end
      end
    end
