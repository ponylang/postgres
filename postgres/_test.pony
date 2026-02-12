use "cli"
use "collections"
use "encode/base64"
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
    test(_TestResponseParserBackendKeyDataMessage)
    test(_TestResponseParserDigitMessageTypeNotJunk)
    test(_TestResponseParserMultipleMessagesParseCompleteFirst)
    test(_TestResponseParserMultipleMessagesBackendKeyDataFirst)
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
    test(_TestFrontendMessageCancelRequest)
    test(_TestCancelQueryInFlight)
    test(_TestSSLCancelQueryInFlight)
    test(_TestCancelPgSleep)
    test(_TestCancelSSLPgSleep)
    test(_TestResponseParserAuthenticationSASLMessage)
    test(_TestResponseParserAuthenticationSASLContinueMessage)
    test(_TestResponseParserAuthenticationSASLFinalMessage)
    test(_TestResponseParserMultipleMessagesSASLFirst)
    test(_TestFrontendMessageSASLInitialResponse)
    test(_TestFrontendMessageSASLResponse)
    test(_TestScramSha256MessageBuilders)
    test(_TestScramSha256ComputeProof)
    test(_TestSCRAMAuthenticationSuccess)
    test(_TestSCRAMUnsupportedMechanism)
    test(_TestSCRAMServerVerificationFailed)
    test(_TestSCRAMErrorDuringAuth)
    test(_TestMD5Authenticate)
    test(_TestMD5AuthenticateFailure)
    test(_TestMD5QueryResults)

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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _AuthenticateTestNotify(h, true))

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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password + " " + info.password,
        info.database),
      _AuthenticateTestNotify(h, false))

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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, true))

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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), host, info.port.reverse()),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, false))

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
  let md5_username: String
  let md5_password: String

  new val create(vars: (Array[String] val | None)) =>
    let e = EnvVars(vars)
    host = try e("POSTGRES_HOST")? else "127.0.0.1" end
    port = try e("POSTGRES_PORT")? else "5432" end
    ssl_host = try e("POSTGRES_SSL_HOST")? else host end
    ssl_port = try e("POSTGRES_SSL_PORT")? else "5433" end
    username = try e("POSTGRES_USERNAME")? else "postgres" end
    password = try e("POSTGRES_PASSWORD")? else "postgres" end
    database = try e("POSTGRES_DATABASE")? else "postgres" end
    md5_username = try e("POSTGRES_MD5_USERNAME")? else "md5user" end
    md5_password = try e("POSTGRES_MD5_PASSWORD")? else "md5pass" end

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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _ConnectTestNotify(h, true))

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

// Cancel query unit test

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

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    is_cancel: Bool)
  =>
    _h = h
    _is_cancel_connection = is_cancel
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if _is_cancel_connection then
      // Verify CancelRequest: 16 bytes total
      // Int32(16) Int32(80877102) Int32(pid=12345) Int32(key=67890)
      if data.size() != 16 then
        _h.fail("CancelRequest should be 16 bytes, got "
          + data.size().string())
        _h.complete(false)
        return
      end

      try
        // Verify length field: big-endian 16
        if (data(0)? != 0) or (data(1)? != 0) or (data(2)? != 0)
          or (data(3)? != 16) then
          _h.fail("CancelRequest length field is incorrect")
          _h.complete(false)
          return
        end

        // Verify magic number: big-endian 80877102 = 0x04D2162E
        if (data(4)? != 4) or (data(5)? != 210) or (data(6)? != 22)
          or (data(7)? != 46) then
          _h.fail("CancelRequest magic number is incorrect")
          _h.complete(false)
          return
        end

        // Verify pid: big-endian 12345 = 0x00003039
        if (data(8)? != 0) or (data(9)? != 0) or (data(10)? != 48)
          or (data(11)? != 57) then
          _h.fail("CancelRequest process_id is incorrect")
          _h.complete(false)
          return
        end

        // Verify key: big-endian 67890 = 0x00010932
        if (data(12)? != 0) or (data(13)? != 1) or (data(14)? != 9)
          or (data(15)? != 50) then
          _h.fail("CancelRequest secret_key is incorrect")
          _h.complete(false)
          return
        end

        _h.complete(true)
      else
        _h.fail("Error reading CancelRequest bytes")
        _h.complete(false)
      end
    else
      if not _authed then
        _authed = true
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let bkd = _IncomingBackendKeyDataTestMessage(12345, 67890).bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        // Send all auth messages in a single write so the Session processes
        // them atomically. If sent separately, TCP may deliver them in
        // different reads, causing ReadyForQuery to arrive after the client
        // has already called cancel() (which would see _QueryNotReady).
        let combined: Array[U8] val = recover val
          let arr = Array[U8]
          arr.append(auth_ok)
          arr.append(bkd)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
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
    if not _ssl_started then
      // SSLRequest — respond 'S' and upgrade to TLS
      let response: Array[U8] val = ['S']
      _tcp_connection.send(response)
      match _tcp_connection.start_tls(_sslctx)
      | None => _ssl_started = true
      | let _: lori.StartTLSError =>
        _tcp_connection.close()
      end
    elseif _is_cancel_connection then
      // Verify CancelRequest: 16 bytes total
      // Int32(16) Int32(80877102) Int32(pid=12345) Int32(key=67890)
      if data.size() != 16 then
        _h.fail("CancelRequest should be 16 bytes, got "
          + data.size().string())
        _h.complete(false)
        return
      end

      try
        if (data(0)? != 0) or (data(1)? != 0) or (data(2)? != 0)
          or (data(3)? != 16) then
          _h.fail("CancelRequest length field is incorrect")
          _h.complete(false)
          return
        end

        if (data(4)? != 4) or (data(5)? != 210) or (data(6)? != 22)
          or (data(7)? != 46) then
          _h.fail("CancelRequest magic number is incorrect")
          _h.complete(false)
          return
        end

        if (data(8)? != 0) or (data(9)? != 0) or (data(10)? != 48)
          or (data(11)? != 57) then
          _h.fail("CancelRequest process_id is incorrect")
          _h.complete(false)
          return
        end

        if (data(12)? != 0) or (data(13)? != 1) or (data(14)? != 9)
          or (data(15)? != 50) then
          _h.fail("CancelRequest secret_key is incorrect")
          _h.complete(false)
          return
        end

        _h.complete(true)
      else
        _h.fail("Error reading CancelRequest bytes")
        _h.complete(false)
      end
    else
      if not _authed then
        _authed = true
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let bkd = _IncomingBackendKeyDataTestMessage(12345, 67890).bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        // Send all auth messages in a single write so the Session processes
        // them atomically (same reason as _CancelTestServer).
        let combined: Array[U8] val = recover val
          let arr = Array[U8]
          arr.append(auth_ok)
          arr.append(bkd)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
      end
      // After auth, receive query data and hold (don't respond)
    end

// SCRAM-SHA-256 authentication unit tests

class \nodoc\ iso _TestSCRAMAuthenticationSuccess is UnitTest
  """
  Verifies the full SCRAM-SHA-256 authentication handshake: AuthSASL →
  SASLInitialResponse → SASLContinue → SASLResponse → SASLFinal → AuthOk →
  ReadyForQuery → pg_session_authenticated.
  """
  fun name(): String =>
    "SCRAM/AuthenticationSuccess"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7677"

    let listener = _SCRAMTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      false)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMUnsupportedMechanism is UnitTest
  """
  Verifies that when the server offers only unsupported SASL mechanisms,
  the session fires pg_session_authentication_failed with
  UnsupportedAuthenticationMethod.
  """
  fun name(): String =>
    "SCRAM/UnsupportedMechanism"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7678"

    let listener = _SCRAMUnsupportedTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMServerVerificationFailed is UnitTest
  """
  Verifies that when the server sends an incorrect signature in SASLFinal,
  the session fires pg_session_authentication_failed with
  ServerVerificationFailed.
  """
  fun name(): String =>
    "SCRAM/ServerVerificationFailed"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7679"

    let listener = _SCRAMTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      true)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMErrorDuringAuth is UnitTest
  """
  Verifies that when the server sends an ErrorResponse with code 28P01
  during SCRAM authentication, the session fires
  pg_session_authentication_failed with InvalidPassword.
  """
  fun name(): String =>
    "SCRAM/ErrorDuringAuth"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7681"

    let listener = _SCRAMErrorTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _SCRAMSuccessTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.close()
    _h.complete(true)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication should have succeeded.")
    _h.complete(false)

actor \nodoc\ _SCRAMFailureTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _expected_reason: AuthenticationFailureReason

  new create(h: TestHelper, expected: AuthenticationFailureReason) =>
    _h = h
    _expected_reason = expected

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    if reason is _expected_reason then
      _h.complete(true)
    else
      _h.fail("Wrong failure reason.")
      _h.complete(false)
    end

actor \nodoc\ _SCRAMTestListener is lori.TCPListenerActor
  """
  Listener for SCRAM-SHA-256 authentication tests. Creates a mock SCRAM
  server and connects a client session. Used by both the success test and
  the server verification failure test.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _send_wrong_signature: Bool

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    send_wrong_signature: Bool)
  =>
    _host = host
    _port = port
    _h = h
    _send_wrong_signature = send_wrong_signature
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SCRAMTestServer =>
    _SCRAMTestServer(_server_auth, fd, _h, _send_wrong_signature)

  fun ref _on_listening() =>
    let notify: SessionStatusNotify = if _send_wrong_signature then
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed)
    else
      _SCRAMSuccessTestNotify(_h)
    end
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that performs a SCRAM-SHA-256 authentication handshake.
  Optionally sends a wrong server signature to test verification failure.

  The SCRAM exchange uses a fixed server nonce suffix, salt, and iteration
  count. The expected client proof and server signature are computed using
  _ScramSha256 with the test password "postgres".
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  let _send_wrong_signature: Bool
  var _received_count: USize = 0
  var _server_signature: (Array[U8] val | None) = None

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    send_wrong_signature: Bool)
  =>
    _h = h
    _send_wrong_signature = send_wrong_signature
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let data_val: Array[U8] val = consume data
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup message: send AuthSASL with ["SCRAM-SHA-256"]
      let mechanisms: Array[String] val = recover val ["SCRAM-SHA-256"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    elseif _received_count == 2 then
      // SASLInitialResponse: parse client nonce, compute SCRAM values,
      // send SASLContinue.
      //
      // Wire format: 'p' I32(len) "SCRAM-SHA-256\0" I32(resp_len) response
      //   response starts at offset 23 (1 type + 4 len + 14 mech\0 + 4 resp_len)
      //   response = "n,,n=,r=<nonce>"
      //   client_first_bare starts at offset 26 (23 + 3, skip "n,,")
      //   client_nonce starts at offset 31 (23 + 8, skip "n,,n=,r=")
      try
        let client_nonce = recover val
          let s = String(data_val.size() - 31)
          var i: USize = 31
          while i < data_val.size() do
            s.push(data_val(i)?)
            i = i + 1
          end
          s
        end

        let client_first_bare = recover val
          let s = String(data_val.size() - 26)
          var i: USize = 26
          while i < data_val.size() do
            s.push(data_val(i)?)
            i = i + 1
          end
          s
        end

        let server_nonce = "servernonce123456"
        let combined_nonce: String val = client_nonce + server_nonce
        let salt: Array[U8] val =
          [0x73; 0x61; 0x6C; 0x74; 0x30; 0x31; 0x32; 0x33]
        let salt_b64_iso = Base64.encode(salt)
        let salt_b64: String val = consume salt_b64_iso
        let iterations: U32 = 4096

        let server_first: String val =
          "r=" + combined_nonce + ",s=" + salt_b64 + ",i=4096"

        (let client_proof, let sig) =
          _ScramSha256.compute_proof("postgres", salt, iterations,
            client_first_bare, server_first, combined_nonce)?

        // client_proof is unused — we don't verify the client's proof in this
        // mock server; the test validates the client's behavior, not ours.
        client_proof.size()

        _server_signature = sig

        let sasl_continue = _IncomingAuthenticationSASLContinueTestMessage(
          server_first.array()).bytes()
        _tcp_connection.send(sasl_continue)
      else
        _h.fail("SCRAM server computation failed")
        _h.complete(false)
      end
    elseif _received_count == 3 then
      // SASLResponse: send SASLFinal + AuthOk + ReadyForQuery
      match _server_signature
      | let sig: Array[U8] val =>
        let final_sig = if _send_wrong_signature then
          recover val Array[U8].init(0, 32) end
        else
          sig
        end
        let sig_b64_iso = Base64.encode(final_sig)
        let sig_b64: String val = consume sig_b64_iso
        let server_final: String val = "v=" + sig_b64
        let sasl_final = _IncomingAuthenticationSASLFinalTestMessage(
          server_final.array()).bytes()
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        let combined = recover val
          let arr = Array[U8]
          arr.append(sasl_final)
          arr.append(auth_ok)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
      end
    end

actor \nodoc\ _SCRAMUnsupportedTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _SCRAMUnsupportedTestServer =>
    _SCRAMUnsupportedTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, UnsupportedAuthenticationMethod))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMUnsupportedTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that sends AuthSASL with only unsupported mechanisms.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _authed then
      _authed = true
      let mechanisms: Array[String] val =
        recover val ["SCRAM-SHA-256-PLUS"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    end

actor \nodoc\ _SCRAMErrorTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _SCRAMErrorTestServer =>
    _SCRAMErrorTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, InvalidPassword))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMErrorTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that starts a SCRAM exchange then sends an ErrorResponse
  with code 28P01 (invalid password) after receiving SASLInitialResponse.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received_count: USize = 0

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup: send AuthSASL with ["SCRAM-SHA-256"]
      let mechanisms: Array[String] val =
        recover val ["SCRAM-SHA-256"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    elseif _received_count == 2 then
      // SASLInitialResponse: send ErrorResponse 28P01
      let err = _IncomingErrorResponseTestMessage("FATAL", "28P01",
        "password authentication failed").bytes()
      _tcp_connection.send(err)
    end

// MD5 backward-compatibility integration tests

class \nodoc\ iso _TestMD5Authenticate is UnitTest
  """
  Verifies that the driver can authenticate using MD5 with a user configured
  for MD5 authentication. The CI PostgreSQL container routes the md5user to
  MD5 auth via pg_hba.conf while the default is SCRAM-SHA-256. Uses the SSL
  container (which has the md5user init script) but connects without SSL.
  """
  fun name(): String =>
    "integration/MD5/Authenticate"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host,
        info.ssl_port),
      DatabaseConnectInfo(info.md5_username, info.md5_password, info.database),
      _AuthenticateTestNotify(h, true))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestMD5AuthenticateFailure is UnitTest
  """
  Verifies that MD5 authentication failure is handled correctly when the
  wrong password is provided for the md5user.
  """
  fun name(): String =>
    "integration/MD5/AuthenticateFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host,
        info.ssl_port),
      DatabaseConnectInfo(info.md5_username, "wrongpassword", info.database),
      _AuthenticateTestNotify(h, false))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestMD5QueryResults is UnitTest
  """
  Verifies that after MD5 authentication, the driver can execute a query
  and receive results.
  """
  fun name(): String =>
    "integration/MD5/QueryResults"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _MD5QueryResultsReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host,
        info.ssl_port),
      DatabaseConnectInfo(info.md5_username, info.md5_password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _MD5QueryResultsReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT 42::text")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate with MD5 user")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    if result.query() isnt _query then
      _h.fail("Query in result isn't the expected query.")
      _h.complete(false)
      return
    end

    match result
    | let r: ResultSet =>
      if r.rows().size() != 1 then
        _h.fail("Wrong number of result rows.")
        _h.complete(false)
        return
      end

      try
        match r.rows()(0)?.fields(0)?.value
        | let v: String =>
          if v != "42" then
            _h.fail("Unexpected query results.")
            _h.complete(false)
            return
          end
        else
          _h.fail("Unexpected query results.")
          _h.complete(false)
          return
        end
      else
        _h.fail("Unexpected error accessing result rows.")
        _h.complete(false)
        return
      end
    else
      _h.fail("Wrong result type.")
      _h.complete(false)
      return
    end

    _h.complete(true)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)
