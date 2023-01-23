use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestQueryResultsIncludeOriginatingQuery is UnitTest
  fun name(): String =>
    "integration/Query/ResultsIncludeOriginatingQuery"

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

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

class \nodoc\ iso _TestQueryAfterAuthenticationFailure is UnitTest
  """
  Test querying after an authetication failure.
  """
  fun name(): String =>
    "integration/Query/AfterAuthenticationFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _QueryAfterAuthenticationFailureNotify(h),
      info.host,
      info.port,
      info.username,
      info.password + " " + info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _QueryAfterAuthenticationFailureNotify is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("select * from free_candy")

  be pg_session_authenticated(session: Session) =>
    _h.fail("Unexpected successful authentication")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    session.execute(_query, this)

  be pg_query_result(result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    if (query is _query) and (failure is SessionClosed) then
      _h.complete(true)
    else
      _h.complete(false)
    end

class \nodoc\ iso _TestQueryAfterConnectionFailure is UnitTest
  """
  Test to verify that querying after connection failures are handled correctly.
  Currently, we set up a bad connect attempt by taking the valid port number that would allow a connect and reversing it to create an attempt to connect on a port that nothing should be listening on.
  """
  fun name(): String =>
    "integration/Query/AfterConnectionFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _QueryAfterConnectionFailureNotify(h),
      info.host,
      info.port.reverse(),
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _QueryAfterConnectionFailureNotify is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("select * from free_candy")

  be pg_session_connected(session: Session) =>
    _h.fail("Unexpected successful connection")
    _h.complete(false)

  be pg_session_connection_failed(session: Session) =>
    session.execute(_query, this)

  be pg_query_result(result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    if (query is _query) and (failure is SessionClosed) then
      _h.complete(true)
    else
      _h.complete(false)
    end

class \nodoc\ iso _TestQueryBeforeAuthentication is UnitTest
  """
  Test querying before authentication.
  """
  // TODO SEAN this test has a race condition in that we can't guarantee that
  // the query execution will happen before authentication is succesful. It
  // will almost always happen in the corret order but there is no guarantee.
  fun name(): String =>
    "integration/Query/BeforeAuthentication"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _QueryBeforeAuthenticationNotify(h),
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _QueryBeforeAuthenticationNotify is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("select * from free_candy")


  be pg_session_connected(session: Session) =>
    session.execute(_query, this)

  be pg_session_connection_failed(session: Session) =>
    _h.fail("Unexpected failed connection")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    if (query is _query) and (failure is SessionNotAuthenticated) then
      _h.complete(true)
    else
      _h.complete(false)
    end
