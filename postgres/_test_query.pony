use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestQueryResults is UnitTest
  fun name(): String =>
    "integration/Query/Results"

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
    h.long_test(5_000_000_000)

actor \nodoc\ _ResultsIncludeOriginatingQueryReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT 525600::text")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    if result.query() isnt _query then
      _h.fail("Query in result isn't the expected query.")
      _h.complete(false)
      return
    end

    if result.rows().size() != 1 then
      _h.fail("Wrong number of result rows.")
      _h.complete(false)
      return
    end

    try
      match result.rows()(0)?.fields(0)?.value
      | let v: String =>
        if v != "525600" then
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

    _h.complete(true)

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

class \nodoc\ iso _TestQueryAfterSessionHasBeenClosed is UnitTest
  """
  Test querying after we've closed the session.
  """
  fun name(): String =>
    "integration/Query/AfterSessionHasBeenClosed"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      _QueryAfterSessionHasBeenClosedNotify(h),
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _QueryAfterSessionHasBeenClosedNotify is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("select * from free_candy")

  be pg_session_authenticated(session: Session) =>
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unexpected authentication failure")

  be pg_session_shutdown(session: Session) =>
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

_TestQueryOfNonExistentTable

class \nodoc\ iso _TestQueryOfNonExistentTable is UnitTest
  fun name(): String =>
    "integration/Query/OfNonExistentTable"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _NonExistentTableQueryReceiver(h)

    let session = Session(
      lori.TCPConnectAuth(h.env.root),
      client,
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _NonExistentTableQueryReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT * from THIS_TABLE_DOESNT_EXIST")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    _h.fail("Query unexpectedly succeeded.")
    _h.complete(false)

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    if query is _query then
      _h.complete(true)
    else
      _h.fail("Incorrect query paramter received.")
      _h.complete(false)
    end

class \nodoc\ _TestCreateAndDropTable is UnitTest
  """
  Tests expectations around client API for creating a table and dropping a
  table.
  """
  fun name(): String =>
    "integration/Query/CreateAndDropTable"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let queries = recover iso
      Array[SimpleQuery]
        .>push(
          SimpleQuery(
            """
            CREATE TABLE CreateAndDropTable (
            fu VARCHAR(50) NOT NULL,
            bar VARCHAR(50) NOT NULL
            )
            """))
        .>push(SimpleQuery("DROP TABLE CreateAndDropTable"))
    end

    let client = _AllSuccessQueryRunningClient(h, info, consume queries)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

class \nodoc\ _TestInsertAndDelete is UnitTest
  """
  Tests expectations around client API for creating inserting records into a
  table and then deleting them.
  """
  fun name(): String =>
    "integration/Query/InsertAndDelete"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let queries = recover iso
      Array[SimpleQuery]
        .>push(
          SimpleQuery(
            """
            CREATE TABLE i_and_d (
            fu VARCHAR(50) NOT NULL,
            bar VARCHAR(50) NOT NULL
            )
            """))
        .>push(SimpleQuery(
          "INSERT INTO i_and_d (fu, bar) VALUES ('fu', 'bar')"))
        .>push(SimpleQuery(
          "INSERT INTO i_and_d (fu, bar) VALUES('pony', 'lang')"))
        .>push(SimpleQuery("DELETE FROM i_and_d"))
        .>push(SimpleQuery("DROP TABLE i_and_d"))
    end

    let client = _AllSuccessQueryRunningClient(h, info, consume queries)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _AllSuccessQueryRunningClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _queries: Array[SimpleQuery]
  let _session: Session

  new create(h: TestHelper,
    info: _ConnectionTestConfiguration,
    queries: Array[SimpleQuery] iso)
  =>
    _h = h
    _queries = consume queries

    _session = Session(
      lori.TCPConnectAuth(h.env.root),
      this,
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

  be pg_session_authenticated(session: Session) =>
    try
      let q = _queries(0)?
      session.execute(q, this)
    else
      _h.fail("Unexpected failure trying to run first query.")
      _h.complete(false)
    end

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    try
      let q = _queries.shift()?
      if result.query() is q then
        if _queries.size() > 0 then
          _session.execute(_queries(0)?, this)
        else
          _h.complete(true)
        end
      end
    else
      _h.fail("Unexpected failure to validate query results.")
      _h.complete(false)
    end

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    _h.fail("Unexpected for query: " + query.string)
    _h.complete(false)

  be dispose() =>
    _session.close()
