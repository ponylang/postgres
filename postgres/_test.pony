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
    test(_TestHandlingJunkMessages)
    test(_TestMessagePassword)
    test(_TestMessageStartup)
    test(_TestResponseParserAuthenticationMD5PasswordMessage)
    test(_TestResponseParserAuthenticationOkMessage)
    test(_TestResponseParserEmptyBuffer)
    test(_TestResponseParserErrorResponseMessage)
    test(_TestResponseParserIncompleteMessage)
    test(_TestResponseParserJunkMessage)
    test(_TestResponseParserMultipleMessagesAuthenticationMD5PasswordFirst)
    test(_TestResponseParserMultipleMessagesAuthenticationOkFirst)
    test(_TestResponseParserMultipleMessagesErrorResponseFirst)
    test(_TestQueryAfterAuthenticationFailure)
    test(_TestQueryAfterConnectionFailure)
    test(_TestQueryAfterSessionHasBeenClosed)
    test(_TestQueryBeforeAuthentication)
    test(_TestQueryResultsIncludeOriginatingQuery)

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

actor \nodoc\ _JunkSendingTestServer is lori.TCPServerActor
  """
  Sends junk "postgres messages" in reponse to any incoming activity. This actor
  is used to test that our client handles getting junk correctly.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this)
    let junk = _IncomingJunkTestMessage.bytes()
    _tcp_connection.send(junk)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let junk = _IncomingJunkTestMessage.bytes()
    _tcp_connection.send(junk)
