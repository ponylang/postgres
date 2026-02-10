use "cli"
use "collections"
use lori = "lori"
use "pony_test"

actor \nodoc\ Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)

  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_TestAuthenticate)
    test(_TestAuthenticateFailure)
    test(_TestConnect)
    test(_TestConnectFailure)
    test(_TestCreateAndDropTable)
    test(_TestEmptyQuery)
    test(_TestHandlingJunkMessages)
    test(_TestInsertAndDelete)
    test(_TestFrontendMessagePassword)
    test(_TestFrontendMessageQuery)
    test(_TestFrontendMessageStartup)
    test(_TestQueryAfterAuthenticationFailure)
    test(_TestQueryAfterConnectionFailure)
    test(_TestQueryAfterSessionHasBeenClosed)
    test(_TestQueryResults)
    test(_TestQueryOfNonExistentTable)
    test(_TestResponseParserAuthenticationMD5PasswordMessage)
    test(_TestResponseParserAuthenticationOkMessage)
    test(_TestResponseParserCommandCompleteMessage)
    test(_TestResponseParserDataRowMessage)
    test(_TestResponseParserEmptyBuffer)
    test(_TestResponseParserEmptyQueryResponseMessage)
    test(_TestResponseParserErrorResponseMessage)
    test(_TestResponseParserIncompleteMessage)
    test(_TestResponseParserJunkMessage)
    test(_TestResponseParserMultipleMessagesAuthenticationMD5PasswordFirst)
    test(_TestResponseParserMultipleMessagesAuthenticationOkFirst)
    test(_TestResponseParserMultipleMessagesErrorResponseFirst)
    test(_TestResponseParserReadyForQueryMessage)
    test(_TestResponseParserRowDescriptionMessage)
    test(_TestResponseParserParseCompleteMessage)
    test(_TestResponseParserBindCompleteMessage)
    test(_TestResponseParserNoDataMessage)
    test(_TestResponseParserCloseCompleteMessage)
    test(_TestResponseParserParameterDescriptionMessage)
    test(_TestResponseParserPortalSuspendedMessage)
    test(_TestResponseParserDigitMessageTypeNotJunk)
    test(_TestResponseParserMultipleMessagesParseCompleteFirst)
    test(_TestFrontendMessageParse)
    test(_TestFrontendMessageParseWithTypes)
    test(_TestFrontendMessageBind)
    test(_TestFrontendMessageBindWithNull)
    test(_TestFrontendMessageDescribePortal)
    test(_TestFrontendMessageExecute)
    test(_TestFrontendMessageSync)
    test(_TestUnansweredQueriesFailOnShutdown)
    test(_TestZeroRowSelectReturnsResultSet)
    test(_TestZeroRowSelect)
    test(_TestMultiStatementMixedResults)
    test(_TestPreparedQueryResults)
    test(_TestPreparedQueryNullParam)
    test(_TestPreparedQueryNonExistentTable)
    test(_TestPreparedQueryInsertAndDelete)
    test(_TestPreparedQueryMixedWithSimple)

class \nodoc\ iso _TestAuthenticate is UnitTest
  """
  Test to verify that given correct login information we can authenticate with
  a Postgres server. This test assumes that connecting is working correctly and
  will fail if it isn't.
  """
  fun name(): String =>
    "integration/Authenicate"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _AuthenticateTestNotify(h, true),
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestAuthenticateFailure is UnitTest
  """
  Test to verify when we fail to authenticate with a Postgres server that are
  handling the failure correctly. This test assumes that connecting is working
  correctly and will fail if it isn't.
  """
  fun name(): String =>
    "integration/AuthenicateFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _AuthenticateTestNotify(h, false),
      info.host,
      info.port,
      info.username,
      info.password + " " + info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _AuthenticateTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _success_expected: Bool

  new create(h: TestHelper, success_expected: Bool) =>
    _h = h
    _success_expected = success_expected

  be pg_session_authenticated(session: Session) =>
    _h.complete(_success_expected == true)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.complete(_success_expected == false)

class \nodoc\ iso _TestConnect is UnitTest
  """
  Test to verify that given correct login information that we can connect to
  a Postgres server.
  """
  fun name(): String =>
    "integration/Connect"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h, true),
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestConnectFailure is UnitTest
  """
  Test to verify that connection failures are handled correctly. Currently,
  we set up a bad connect attempt by taking the valid port number that would
  allow a connect and reversing it to create an attempt to connect on a port
  that nothing should be listening on.
  """
  fun name(): String =>
    "integration/ConnectFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h, false),
      info.host,
      info.port.reverse(),
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _ConnectTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _success_expected: Bool

  new create(h: TestHelper, success_expected: Bool) =>
    _h = h
    _success_expected = success_expected

  be pg_session_connected(session: Session) =>
    _h.complete(_success_expected == true)

  be pg_session_connection_failed(session: Session) =>
    _h.complete(_success_expected == false)

class \nodoc\ val _ConnectionTestConfiguration
  let host: String
  let port: String
  let username: String
  let password: String
  let database: String

  new val create(vars: (Array[String] val | None)) =>
    let e = EnvVars(vars)
    host = try e("POSTGRES_HOST")? else "127.0.0.1" end
    port = try e("POSTGRES_PORT")? else "5432" end
    username = try e("POSTGRES_USERNAME")? else "postgres" end
    password = try e("POSTGRES_PASSWORD")? else "postgres" end
    database = try e("POSTGRES_DATABASE")? else "postgres" end

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
      lori.TCPConnectAuth(_h.env.root),
      _HandlingJunkTestNotify(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres")

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

  be pg_query_result(result: Result) =>
    _h.fail("Unexpectedly got a result for a query.")
    _h.complete(false)

  be pg_query_failed(query: Query,
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
      lori.TCPConnectAuth(_h.env.root),
      _DoesntAnswerClient(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres")

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

  be pg_query_result(result: Result) =>
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

  be pg_query_failed(query: Query,
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
      lori.TCPConnectAuth(_h.env.root),
      _ZeroRowSelectTestClient(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres")

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
  var _received_count: USize = 0
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup: send AuthOk + ReadyForQuery(idle)
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    elseif _received_count == 2 then
      // Query: send RowDescription (one text column) + CommandComplete +
      // ReadyForQuery
      try
        let columns: Array[(String, String)] val = recover val
          [("col", "text")]
        end
        let row_desc = _IncomingRowDescriptionTestMessage(columns)?.bytes()
        let cmd_complete = _IncomingCommandCompleteTestMessage("SELECT 0").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(row_desc)
        _tcp_connection.send(cmd_complete)
        _tcp_connection.send(ready)
      end
    end
