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
    else
      _h.fail("Wrong result type.")
      _h.complete(false)
      return
    end

    _h.complete(true)

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
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

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
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

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if (query is _query) and (failure is SessionClosed) then
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

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if (query is _query) and (failure is SessionClosed) then
      _h.complete(true)
    else
      _h.complete(false)
    end

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

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    // TODO enhance this by checking the failure
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

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected for query: " + query.string)
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestEmptyQuery is UnitTest
  fun name(): String =>
    "integration/Query/EmptyQuery"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _EmptyQueryReceiver(h)

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

actor \nodoc\ _EmptyQueryReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("")

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

    _h.complete(true)

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

class \nodoc\ iso _TestZeroRowSelect is UnitTest
  """
  Verifies that a SELECT returning zero rows produces a ResultSet with zero
  rows rather than a RowModifying result.
  """
  fun name(): String =>
    "integration/Query/ZeroRowSelect"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _ZeroRowSelectReceiver(h)

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

actor \nodoc\ _ZeroRowSelectReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT 1 WHERE false")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    if result.query() isnt _query then
      _h.fail("Query in result isn't the expected query.")
      _h.complete(false)
      return
    end

    match result
    | let r: ResultSet =>
      if r.rows().size() != 0 then
        _h.fail("Expected zero rows but got " + r.rows().size().string())
        _h.complete(false)
        return
      end
    else
      _h.fail("Expected ResultSet but got a different result type.")
      _h.complete(false)
      return
    end

    _h.complete(true)

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

class \nodoc\ iso _TestMultiStatementMixedResults is UnitTest
  """
  Verifies correct result types when a multi-statement query produces both
  a zero-row SELECT (ResultSet) and an INSERT (RowModifying).
  """
  fun name(): String =>
    "integration/Query/MultiStatementMixedResults"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _MultiStatementMixedClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _MultiStatementMixedClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      lori.TCPConnectAuth(h.env.root),
      this,
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

  be pg_session_authenticated(session: Session) =>
    // Phase 0: create the table
    _phase = 0
    session.execute(
      SimpleQuery(
        """
        CREATE TABLE mixed_test (col VARCHAR(50) NOT NULL)
        """),
      this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table created, now send multi-statement query
      _session.execute(
        SimpleQuery(
          "SELECT * FROM mixed_test WHERE false; INSERT INTO mixed_test (col) VALUES ('x')"),
        this)
    | 2 =>
      // First result from multi-statement: should be ResultSet (zero rows)
      match result
      | let r: ResultSet =>
        if r.rows().size() != 0 then
          _h.fail(
            "Expected zero rows in SELECT result but got "
              + r.rows().size().string())
          _drop_and_finish()
          return
        end
      else
        _h.fail(
          "Expected ResultSet for zero-row SELECT but got different type.")
        _drop_and_finish()
        return
      end
    | 3 =>
      // Second result from multi-statement: should be RowModifying
      match result
      | let r: RowModifying =>
        if r.impacted() != 1 then
          _h.fail(
            "Expected 1 impacted row but got " + r.impacted().string())
          _drop_and_finish()
          return
        end
      else
        _h.fail(
          "Expected RowModifying for INSERT but got different type.")
        _drop_and_finish()
        return
      end
      // Both results verified, drop table
      _session.execute(SimpleQuery("DROP TABLE mixed_test"), this)
    | 4 =>
      // Table dropped, all done
      _h.complete(true)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _drop_and_finish()
    end

  be pg_query_failed(query: SimpleQuery,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _drop_and_finish()

  fun ref _drop_and_finish() =>
    _session.execute(SimpleQuery("DROP TABLE IF EXISTS mixed_test"), this)
    _h.complete(false)

  be dispose() =>
    _session.close()
