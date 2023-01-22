use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestQueryResultsIncludeOriginatingQuery is UnitTest
  fun name(): String =>
    "integration/Query/QueryResultsIncludeOriginatingQuery"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _ResultsIncludeOriginatingQueryReceiver(h)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      client,
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(1_000_000_000)

actor \nodoc\ _ResultsIncludeOriginatingQueryReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("select * from free_candy")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    _h.complete(result.query() is _query)
