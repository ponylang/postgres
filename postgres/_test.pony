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
    test(_Authenticate)
    test(_AuthenticateFailure)
    test(_Connect)
    test(_ConnectFailure)
    test(_ResponseParserAuthenticationMD5PasswordMessage)
    test(_ResponseParserAuthenticationOkMessage)
    test(_ResponseParserEmptyBuffer)
    test(_ResponseParserErrorResponseMessage)
    test(_ResponseParserIncompleteMessage)
    test(_ResponseParserMultipleMessagesAuthenticationMD5PasswordFirst)
    test(_ResponseParserMultipleMessagesAuthenticationOkFirst)
    test(_ResponseParserMultipleMessagesErrorResponseFirst)

class \nodoc\ iso _Authenticate is UnitTest
  """
  Test to verify that given correct login information we can authenticate with
  a Postgres server. This test assumes that connecting is working correctly and
  will fail if it isn't.
  """
  fun name(): String =>
    "integration/Authenicate"

  fun apply(h: TestHelper) =>
    let info = _TestConnectionConfiguration(h.env.vars)

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

class \nodoc\ iso _AuthenticateFailure is UnitTest
  """
  Test to verify when we fail to authenticate with a Postgres server that are
  handling the failure correctly. This test assumes that connecting is working
  correctly and will fail if it isn't.
  """
  fun name(): String =>
    "integration/AuthenicateFailure"

  fun apply(h: TestHelper) =>
    let info = _TestConnectionConfiguration(h.env.vars)

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
  let _sucess_expected: Bool

  new create(h: TestHelper, sucess_expected: Bool) =>
    _h = h
    _sucess_expected = sucess_expected

  be pg_session_authenticated(session: Session) =>
    _h.complete(_sucess_expected == true)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.complete(_sucess_expected == false)

class \nodoc\ iso _Connect is UnitTest
  """
  Test to verify that given correct login information that we can connect to
  a Postgres server.
  """
  fun name(): String =>
    "integration/Connect"

  fun apply(h: TestHelper) =>
    let info = _TestConnectionConfiguration(h.env.vars)

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

class \nodoc\ iso _ConnectFailure is UnitTest
  """
  Test to verify that connection failures are handled correctly. Currently,
  we set up a bad connect attempt by taking the valid port number that would
  allow a connect and reversing it to create an attempt to connect on a port
  that nothing should be listening on.
  """
  fun name(): String =>
    "integration/ConnectFailure"

  fun apply(h: TestHelper) =>
    let info = _TestConnectionConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _ConnectTestNotify(h,false),
      info.host,
      info.port.reverse(),
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _ConnectTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _sucess_expected: Bool

  new create(h: TestHelper, sucess_expected: Bool) =>
    _h = h
    _sucess_expected = sucess_expected

  be pg_session_connected(session: Session) =>
    _h.complete(_sucess_expected == true)

  be pg_session_connection_failed(session: Session) =>
    _h.complete(_sucess_expected == false)

class \nodoc\ val _TestConnectionConfiguration
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
