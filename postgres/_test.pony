use "cli"
use "collections"
use "files"
use lori = "lori"
use "pony_check"
use "pony_test"
use "ssl/net"

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
    test(_TestFrontendMessageDescribeStatement)
    test(_TestFrontendMessageCloseStatement)
    test(_TestFrontendMessageSync)
    test(_TestFrontendMessageSSLRequest)
    test(_TestFrontendMessageTerminate)
    test(_TestTerminateSentOnClose)
    test(_TestSSLNegotiationRefused)
    test(_TestSSLNegotiationJunkResponse)
    test(_TestSSLNegotiationSuccess)
    test(_TestUnansweredQueriesFailOnShutdown)
    test(_TestPrepareShutdownDrainsPrepareQueue)
    test(_TestZeroRowSelectReturnsResultSet)
    test(_TestZeroRowSelect)
    test(_TestMultiStatementMixedResults)
    test(_TestPreparedQueryResults)
    test(_TestPreparedQueryNullParam)
    test(_TestPreparedQueryNonExistentTable)
    test(_TestPreparedQueryInsertAndDelete)
    test(_TestPreparedQueryMixedWithSimple)
    test(_TestPrepareStatement)
    test(_TestPrepareAndExecute)
    test(_TestPrepareAndExecuteMultiple)
    test(_TestPrepareAndClose)
    test(_TestPrepareFails)
    test(_TestPrepareAfterClose)
    test(_TestCloseNonexistent)
    test(_TestPrepareDuplicateName)
    test(_TestPreparedStatementMixedWithSimpleAndPrepared)
    test(_TestSSLConnect)
    test(_TestSSLAuthenticate)
    test(_TestSSLQueryResults)
    test(_TestSSLRefused)
    test(_TestFieldEqualityReflexive)
    test(_TestFieldEqualityStructural)
    test(_TestFieldEqualitySymmetric)
    test(_TestFieldInequality)
    test(_TestRowEquality)
    test(_TestRowInequality)
    test(_TestRowsEquality)
    test(_TestRowsInequality)
    test(Property1UnitTest[Field](_TestFieldReflexiveProperty))
    test(Property1UnitTest[FieldDataTypes](_TestFieldStructuralProperty))
    test(Property1UnitTest[(FieldDataTypes, FieldDataTypes)](
      _TestFieldSymmetricProperty))
    test(Property1UnitTest[Row](_TestRowReflexiveProperty))
    test(Property1UnitTest[Rows](_TestRowsReflexiveProperty))

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
    let host = ifdef linux then "127.0.0.2" else "localhost" end

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h, false),
      host,
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
  let ssl_host: String
  let ssl_port: String
  let username: String
  let password: String
  let database: String

  new val create(vars: (Array[String] val | None)) =>
    let e = EnvVars(vars)
    host = try e("POSTGRES_HOST")? else "127.0.0.1" end
    port = try e("POSTGRES_PORT")? else "5432" end
    ssl_host = try e("POSTGRES_SSL_HOST")? else host end
    ssl_port = try e("POSTGRES_SSL_PORT")? else "5433" end
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
      lori.TCPConnectAuth(_h.env.root),
      _PrepareShutdownTestClient(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres")

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
      lori.TCPConnectAuth(_h.env.root),
      _TerminateSentTestNotify(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres")

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

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _authed then
      _authed = true
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    else
      try
        if data(0)? == 'X' then
          _h.complete(true)
        end
      end
    end

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
      lori.TCPConnectAuth(_h.env.root),
      _SSLRefusedTestNotify(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres",
      SSLRequired(_sslctx))

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
      lori.TCPConnectAuth(_h.env.root),
      _SSLJunkTestNotify(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres",
      SSLRequired(_sslctx))

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
      lori.TCPConnectAuth(_h.env.root),
      _SSLSuccessTestNotify(_h),
      _host,
      _port,
      "postgres",
      "postgres",
      "postgres",
      SSLRequired(_client_sslctx))

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

  new create(auth: lori.TCPServerAuth, sslctx: SSLContext val, fd: U32) =>
    _sslctx = sslctx
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _ssl_started then
      // Client sent SSLRequest — respond 'S' and upgrade to TLS
      let response: Array[U8] val = ['S']
      _tcp_connection.send(response)
      match _tcp_connection.start_tls(_sslctx)
      | None => _ssl_started = true
      | let _: lori.StartTLSError =>
        _tcp_connection.close()
      end
    else
      // StartupMessage received over TLS — send AuthOk + ReadyForQuery
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    end

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
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h, true),
      info.ssl_host,
      info.ssl_port,
      info.username,
      info.password,
      info.database,
      SSLRequired(sslctx))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSSLAuthenticate is UnitTest
  """
  Verifies that connecting with SSLRequired to a PostgreSQL server with SSL
  enabled allows successful MD5 authentication over the encrypted connection.
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
      lori.TCPConnectAuth(h.env.root),
      _AuthenticateTestNotify(h, true),
      info.ssl_host,
      info.ssl_port,
      info.username,
      info.password,
      info.database,
      SSLRequired(sslctx))

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
      lori.TCPConnectAuth(h.env.root),
      client,
      info.ssl_host,
      info.ssl_port,
      info.username,
      info.password,
      info.database,
      SSLRequired(sslctx))

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
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h, false),
      info.host,
      info.port,
      info.username,
      info.password,
      info.database,
      SSLRequired(sslctx))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)
