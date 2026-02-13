use "cli"
use lori = "lori"
use "pony_check"
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
    test(_TestResponseParserBackendKeyDataMessage)
    test(_TestResponseParserDigitMessageTypeNotJunk)
    test(_TestResponseParserMultipleMessagesParseCompleteFirst)
    test(_TestResponseParserMultipleMessagesBackendKeyDataFirst)
    test(_TestResponseParserParameterStatusSkipped)
    test(_TestResponseParserNoticeResponseSkipped)
    test(_TestResponseParserNotificationResponseMessage)
    test(_TestResponseParserMultipleMessagesAsyncThenAuth)
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
    test(_TestResponseParserUnsupportedAuthenticationMessage)
    test(_TestResponseParserMultipleMessagesSASLFirst)
    test(_TestFrontendMessageSASLInitialResponse)
    test(_TestFrontendMessageSASLResponse)
    test(_TestScramSha256MessageBuilders)
    test(_TestScramSha256ComputeProof)
    test(_TestSCRAMAuthenticationSuccess)
    test(_TestSCRAMUnsupportedMechanism)
    test(_TestSCRAMServerVerificationFailed)
    test(_TestSCRAMErrorDuringAuth)
    test(_TestUnsupportedAuthentication)
    test(_TestMD5Authenticate)
    test(_TestMD5AuthenticateFailure)
    test(_TestMD5QueryResults)
    test(_TestTransactionCommit)
    test(_TestTransactionRollbackAfterFailure)
    test(_TestTransactionStatusOnAuthentication)
    test(_TestTransactionStatusDuringTransaction)
    test(_TestTransactionStatusOnFailedTransaction)
    test(_TestNotificationDelivery)
    test(_TestNotificationDuringDataRows)
    test(_TestListenNotify)
    test(_TestResponseParserCopyInResponseMessage)
    test(_TestFrontendMessageCopyData)
    test(_TestFrontendMessageCopyDone)
    test(_TestFrontendMessageCopyFail)
    test(_TestCopyInSuccess)
    test(_TestCopyInAbort)
    test(_TestCopyInServerError)
    test(_TestCopyInShutdownDrainsCopyQueue)
    test(_TestCopyInAfterSessionClosed)
    test(_TestCopyInInsert)
    test(_TestCopyInAbortRollback)

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
