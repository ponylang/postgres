use lori = "lori"
use "pony_test"

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
