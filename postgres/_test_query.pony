use lori = "lori"
use "pony_test"
use "ssl/net"

class \nodoc\ iso _TestQueryResults is UnitTest
  fun name(): String =>
    "integration/Query/Results"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _ResultsIncludeOriginatingQueryReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

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

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password + " " + info.password,
        info.database),
      _QueryAfterAuthenticationFailureNotify(h))

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

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
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
    let host = ifdef linux then "127.0.0.2" else "localhost" end

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), host, info.port.reverse()),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _QueryAfterConnectionFailureNotify(h))

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

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _QueryAfterSessionHasBeenClosedNotify(h))

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

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Unexpected query result received")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

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

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Query unexpectedly succeeded.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

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

  be pg_query_result(session: Session, result: Result) =>
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

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    let query_str = match query
    | let sq: SimpleQuery => sq.string
    | let pq: PreparedQuery => pq.string
    | let nq: NamedPreparedQuery => nq.name
    end
    _h.fail("Unexpected for query: " + query_str)
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

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

  be pg_query_result(session: Session, result: Result) =>
    if result.query() isnt _query then
      _h.fail("Query in result isn't the expected query.")
      _h.complete(false)
      return
    end

    _h.complete(true)

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

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

  be pg_query_result(session: Session, result: Result) =>
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

  be pg_query_failed(session: Session, query: Query,
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
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

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

  be pg_query_result(session: Session, result: Result) =>
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

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _drop_and_finish()

  fun ref _drop_and_finish() =>
    _session.execute(SimpleQuery("DROP TABLE IF EXISTS mixed_test"), this)
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPreparedQueryResults is UnitTest
  """
  Verifies that a PreparedQuery with a parameter returns the correct result
  through the extended query protocol.
  """
  fun name(): String =>
    "integration/PreparedQuery/Results"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PreparedQueryResultsReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _PreparedQueryResultsReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: PreparedQuery

  new create(h: TestHelper) =>
    _h = h
    _query = PreparedQuery("SELECT $1::text",
      recover val [as (String | None): "525600"] end)

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
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

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

class \nodoc\ iso _TestPreparedQueryNullParam is UnitTest
  """
  Verifies that a PreparedQuery with a NULL parameter correctly produces
  a None field value in the result.
  """
  fun name(): String =>
    "integration/PreparedQuery/NullParam"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PreparedQueryNullParamReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _PreparedQueryNullParamReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: PreparedQuery

  new create(h: TestHelper) =>
    _h = h
    _query = PreparedQuery("SELECT $1::text",
      recover val [as (String | None): None] end)

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
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
        | None => None // expected
        else
          _h.fail("Expected None for NULL parameter but got a value.")
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

class \nodoc\ iso _TestPreparedQueryNonExistentTable is UnitTest
  """
  Verifies that errors from the extended query protocol are correctly
  delivered as pg_query_failed.
  """
  fun name(): String =>
    "integration/PreparedQuery/OfNonExistentTable"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PreparedQueryNonExistentTableReceiver(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _PreparedQueryNonExistentTableReceiver is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: PreparedQuery

  new create(h: TestHelper) =>
    _h = h
    _query = PreparedQuery(
      "SELECT * FROM THIS_TABLE_DOESNT_EXIST WHERE id = $1",
      recover val [as (String | None): "1"] end)

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to establish connection")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Query unexpectedly succeeded.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if query is _query then
      _h.complete(true)
    else
      _h.fail("Incorrect query parameter received.")
      _h.complete(false)
    end

class \nodoc\ iso _TestPreparedQueryInsertAndDelete is UnitTest
  """
  Verifies INSERT and DELETE through the extended query protocol, confirming
  that non-SELECT prepared queries produce RowModifying results with the
  correct impacted row count.
  """
  fun name(): String =>
    "integration/PreparedQuery/InsertAndDelete"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PreparedQueryInsertAndDeleteClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PreparedQueryInsertAndDeleteClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    _phase = 0
    session.execute(
      SimpleQuery(
        """
        CREATE TABLE prep_i_and_d (
        col VARCHAR(50) NOT NULL
        )
        """),
      this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table created, insert with PreparedQuery
      _session.execute(
        PreparedQuery(
          "INSERT INTO prep_i_and_d (col) VALUES ($1)",
          recover val [as (String | None): "hello"] end),
        this)
    | 2 =>
      // Insert done, verify RowModifying with impacted = 1
      match result
      | let r: RowModifying =>
        if r.impacted() != 1 then
          _h.fail(
            "Expected 1 impacted row but got " + r.impacted().string())
          _drop_and_finish()
          return
        end
      else
        _h.fail("Expected RowModifying for INSERT but got different type.")
        _drop_and_finish()
        return
      end
      // Delete with PreparedQuery
      _session.execute(
        PreparedQuery(
          "DELETE FROM prep_i_and_d WHERE col = $1",
          recover val [as (String | None): "hello"] end),
        this)
    | 3 =>
      // Delete done, verify RowModifying with impacted = 1
      match result
      | let r: RowModifying =>
        if r.impacted() != 1 then
          _h.fail(
            "Expected 1 impacted row but got " + r.impacted().string())
          _drop_and_finish()
          return
        end
      else
        _h.fail("Expected RowModifying for DELETE but got different type.")
        _drop_and_finish()
        return
      end
      // Drop table
      _session.execute(SimpleQuery("DROP TABLE prep_i_and_d"), this)
    | 4 =>
      _h.complete(true)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _drop_and_finish()
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _drop_and_finish()

  fun ref _drop_and_finish() =>
    _session.execute(
      SimpleQuery("DROP TABLE IF EXISTS prep_i_and_d"), this)
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPreparedQueryMixedWithSimple is UnitTest
  """
  Verifies that SimpleQuery and PreparedQuery can be executed in sequence
  within the same session, confirming the state machine correctly alternates
  between the simple and extended query protocols.
  """
  fun name(): String =>
    "integration/PreparedQuery/MixedWithSimple"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PreparedQueryMixedClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PreparedQueryMixedClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    // Phase 0: SimpleQuery
    _phase = 0
    session.execute(SimpleQuery("SELECT 525600::text"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // SimpleQuery result, verify and send PreparedQuery
      match result
      | let r: ResultSet =>
        try
          match r.rows()(0)?.fields(0)?.value
          | let v: String =>
            if v != "525600" then
              _h.fail("Unexpected SimpleQuery result: " + v)
              _h.complete(false)
              return
            end
          else
            _h.fail("Unexpected SimpleQuery result type.")
            _h.complete(false)
            return
          end
        else
          _h.fail("Error accessing SimpleQuery result rows.")
          _h.complete(false)
          return
        end
      else
        _h.fail("Expected ResultSet from SimpleQuery.")
        _h.complete(false)
        return
      end
      // Now send PreparedQuery
      _session.execute(
        PreparedQuery("SELECT $1::text",
          recover val [as (String | None): "42"] end),
        this)
    | 2 =>
      // PreparedQuery result, verify
      match result
      | let r: ResultSet =>
        try
          match r.rows()(0)?.fields(0)?.value
          | let v: String =>
            if v != "42" then
              _h.fail("Unexpected PreparedQuery result: " + v)
              _h.complete(false)
              return
            end
          else
            _h.fail("Unexpected PreparedQuery result type.")
            _h.complete(false)
            return
          end
        else
          _h.fail("Error accessing PreparedQuery result rows.")
          _h.complete(false)
          return
        end
      else
        _h.fail("Expected ResultSet from PreparedQuery.")
        _h.complete(false)
        return
      end
      _h.complete(true)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _h.complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareStatement is UnitTest
  """
  Verifies that Session.prepare() successfully prepares a named statement
  and delivers pg_statement_prepared.
  """
  fun name(): String =>
    "integration/PreparedStatement/Prepare"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareStatementClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareStatementClient is
  (SessionStatusNotify & PrepareReceiver)
  let _h: TestHelper
  let _session: Session

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.prepare("s1", "SELECT $1::text", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    if name == "s1" then
      _h.complete(true)
    else
      _h.fail("Unexpected statement name: " + name)
      _h.complete(false)
    end

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure")
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareAndExecute is UnitTest
  """
  Verifies prepare then execute with NamedPreparedQuery returns correct
  results.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareAndExecute"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareAndExecuteClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareAndExecuteClient is
  (SessionStatusNotify & PrepareReceiver & ResultReceiver)
  let _h: TestHelper
  let _session: Session

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.prepare("s1", "SELECT $1::text", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _session.execute(
      NamedPreparedQuery("s1",
        recover val [as (String | None): "525600"] end),
      this)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      try
        match r.rows()(0)?.fields(0)?.value
        | let v: String =>
          if v == "525600" then
            _h.complete(true)
            return
          end
          _h.fail("Unexpected result: " + v)
        else
          _h.fail("Unexpected result type.")
        end
      else
        _h.fail("Error accessing result rows.")
      end
    else
      _h.fail("Expected ResultSet.")
    end
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareAndExecuteMultiple is UnitTest
  """
  Verifies preparing once and executing twice with different parameters.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareAndExecuteMultiple"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareAndExecuteMultipleClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareAndExecuteMultipleClient is
  (SessionStatusNotify & PrepareReceiver & ResultReceiver)
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.prepare("s1", "SELECT $1::text", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _session.execute(
      NamedPreparedQuery("s1",
        recover val [as (String | None): "first"] end),
      this)
    _session.execute(
      NamedPreparedQuery("s1",
        recover val [as (String | None): "second"] end),
      this)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match result
    | let r: ResultSet =>
      try
        match r.rows()(0)?.fields(0)?.value
        | let v: String =>
          match _phase
          | 1 =>
            if v != "first" then
              _h.fail("Expected 'first' but got '" + v + "'")
              _h.complete(false)
              return
            end
          | 2 =>
            if v != "second" then
              _h.fail("Expected 'second' but got '" + v + "'")
              _h.complete(false)
              return
            end
            _h.complete(true)
            return
          end
        else
          _h.fail("Unexpected result type.")
          _h.complete(false)
          return
        end
      else
        _h.fail("Error accessing result rows.")
        _h.complete(false)
        return
      end
    else
      _h.fail("Expected ResultSet.")
      _h.complete(false)
      return
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareAndClose is UnitTest
  """
  Verifies that closing a prepared statement and then executing it produces
  a server error.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareAndClose"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareAndCloseClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareAndCloseClient is
  (SessionStatusNotify & PrepareReceiver & ResultReceiver)
  let _h: TestHelper
  let _session: Session

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.prepare("s1", "SELECT $1::text", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _session.close_statement("s1")
    _session.execute(
      NamedPreparedQuery("s1",
        recover val [as (String | None): "hello"] end),
      this)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected query failure after closing statement.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage => _h.complete(true)
    else
      _h.fail("Expected ErrorResponseMessage but got ClientQueryError.")
      _h.complete(false)
    end

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareFails is UnitTest
  """
  Verifies that preparing invalid SQL delivers pg_prepare_failed with an
  ErrorResponseMessage.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareFails"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareFailsClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareFailsClient is
  (SessionStatusNotify & PrepareReceiver)
  let _h: TestHelper
  let _session: Session

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.prepare("bad", "NOT VALID SQL !!!", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _h.fail("Expected prepare to fail but it succeeded.")
    _h.complete(false)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if name != "bad" then
      _h.fail("Unexpected statement name: " + name)
      _h.complete(false)
      return
    end
    match failure
    | let e: ErrorResponseMessage => _h.complete(true)
    else
      _h.fail("Expected ErrorResponseMessage but got ClientQueryError.")
      _h.complete(false)
    end

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareAfterClose is UnitTest
  """
  Verifies that after closing a named statement, re-preparing the same name
  succeeds.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareAfterClose"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareAfterCloseClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareAfterCloseClient is
  (SessionStatusNotify & PrepareReceiver & ResultReceiver)
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    _phase = 0
    session.prepare("s1", "SELECT 1::text", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      _session.close_statement("s1")
      _session.prepare("s1", "SELECT 42::text", this)
    | 2 =>
      _session.execute(
        NamedPreparedQuery("s1", recover val Array[(String | None)] end),
        this)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _h.complete(false)
    end

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure at phase " + _phase.string())
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      try
        match r.rows()(0)?.fields(0)?.value
        | let v: String =>
          if v == "42" then
            _h.complete(true)
            return
          end
          _h.fail("Expected '42' but got '" + v + "'")
        else
          _h.fail("Unexpected result type.")
        end
      else
        _h.fail("Error accessing result rows.")
      end
    else
      _h.fail("Expected ResultSet.")
    end
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure")
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestCloseNonexistent is UnitTest
  """
  Verifies that closing a statement that was never prepared does not cause
  an error. Fire-and-forget, so we run a query afterward to confirm the
  session still works.
  """
  fun name(): String =>
    "integration/PreparedStatement/CloseNonexistent"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _CloseNonexistentClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _CloseNonexistentClient is
  (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _session: Session

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.close_statement("nonexistent")
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.complete(true)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure after closing nonexistent statement.")
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPrepareDuplicateName is UnitTest
  """
  Verifies that preparing the same name twice without closing produces a
  server error on the second prepare.
  """
  fun name(): String =>
    "integration/PreparedStatement/PrepareDuplicateName"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _PrepareDuplicateNameClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _PrepareDuplicateNameClient is
  (SessionStatusNotify & PrepareReceiver)
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    _phase = 0
    session.prepare("dup", "SELECT 1", this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _phase = _phase + 1
    if _phase == 1 then
      _session.prepare("dup", "SELECT 2", this)
    else
      _h.fail("Second prepare should have failed.")
      _h.complete(false)
    end

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _phase = _phase + 1
    if _phase == 2 then
      match failure
      | let e: ErrorResponseMessage => _h.complete(true)
      else
        _h.fail("Expected ErrorResponseMessage for duplicate prepare.")
        _h.complete(false)
      end
    else
      _h.fail("Unexpected prepare failure at phase " + _phase.string())
      _h.complete(false)
    end

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestPreparedStatementMixedWithSimpleAndPrepared is UnitTest
  """
  Verifies that SimpleQuery, PreparedQuery, and NamedPreparedQuery can all
  be interleaved within the same session.
  """
  fun name(): String =>
    "integration/PreparedStatement/MixedWithSimpleAndPrepared"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _MixedAllThreeClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _MixedAllThreeClient is
  (SessionStatusNotify & PrepareReceiver & ResultReceiver)
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    _phase = 0
    session.execute(SimpleQuery("SELECT 'simple'::text"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_statement_prepared(session: Session, name: String) =>
    _phase = _phase + 1
    _session.execute(
      NamedPreparedQuery("mix",
        recover val [as (String | None): "named"] end),
      this)

  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected prepare failure at phase " + _phase.string())
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match result
    | let r: ResultSet =>
      try
        match r.rows()(0)?.fields(0)?.value
        | let v: String =>
          match _phase
          | 1 =>
            if v != "simple" then
              _h.fail("Expected 'simple' but got '" + v + "'")
              _h.complete(false)
              return
            end
            _session.execute(
              PreparedQuery("SELECT $1::text",
                recover val [as (String | None): "prepared"] end),
              this)
          | 2 =>
            if v != "prepared" then
              _h.fail("Expected 'prepared' but got '" + v + "'")
              _h.complete(false)
              return
            end
            _session.prepare("mix", "SELECT $1::text", this)
          | 4 =>
            if v != "named" then
              _h.fail("Expected 'named' but got '" + v + "'")
              _h.complete(false)
              return
            end
            _h.complete(true)
          else
            _h.fail("Unexpected phase " + _phase.string())
            _h.complete(false)
          end
        else
          _h.fail("Unexpected result type at phase " + _phase.string())
          _h.complete(false)
        end
      else
        _h.fail("Error accessing result rows at phase " + _phase.string())
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet at phase " + _phase.string())
      _h.complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _h.complete(false)

  be dispose() =>
    _session.close()

// Cancel integration test

class \nodoc\ iso _TestCancelPgSleep is UnitTest
  """
  Verifies that cancelling a long-running query on a real PostgreSQL server
  produces a query failure with SQLSTATE 57014 (query_canceled).
  """
  fun name(): String =>
    "integration/Cancel/Query"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _CancelPgSleepClient(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _CancelPgSleepClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _query: SimpleQuery

  new create(h: TestHelper) =>
    _h = h
    _query = SimpleQuery("SELECT pg_sleep(30)")

  be pg_session_authenticated(session: Session) =>
    session.execute(_query, this)
    session.cancel()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _h.fail("Expected query to be cancelled, but got a result.")
    _h.complete(false)

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if query isnt _query then
      _h.fail("Got failure for unexpected query.")
      _h.complete(false)
      return
    end

    match failure
    | let err: ErrorResponseMessage =>
      if err.code == "57014" then
        _h.complete(true)
      else
        _h.fail("Expected SQLSTATE 57014 but got " + err.code)
        _h.complete(false)
      end
    | let ce: ClientQueryError =>
      _h.fail("Expected ErrorResponseMessage but got ClientQueryError.")
      _h.complete(false)
    end

// SSL cancel integration test

class \nodoc\ iso _TestCancelSSLPgSleep is UnitTest
  """
  Verifies that cancelling a long-running query on a real PostgreSQL server
  over an SSL-encrypted connection produces a query failure with SQLSTATE
  57014 (query_canceled).
  """
  fun name(): String =>
    "integration/SSL/Cancel"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let sslctx = recover val
      SSLContext
        .> set_client_verify(false)
        .> set_server_verify(false)
    end

    let client = _CancelPgSleepClient(h)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.ssl_host, info.ssl_port, SSLRequired(sslctx)),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

// Explicit transaction tests

class \nodoc\ iso _TestTransactionCommit is UnitTest
  """
  Verifies that an explicit transaction (BEGIN, INSERT, COMMIT) completes
  successfully. This exercises the bug fix where non-idle ReadyForQuery
  responses (status 'T') previously stalled the query queue.
  """
  fun name(): String =>
    "integration/Transaction/Commit"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _TransactionCommitClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _TransactionCommitClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    // Phase 0: create the table
    _phase = 0
    session.execute(
      SimpleQuery(
        """
        CREATE TABLE txn_commit_test (col VARCHAR(50) NOT NULL)
        """),
      this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table created, BEGIN
      _session.execute(SimpleQuery("BEGIN"), this)
    | 2 =>
      // In transaction, INSERT
      _session.execute(
        SimpleQuery(
          "INSERT INTO txn_commit_test (col) VALUES ('hello')"),
        this)
    | 3 =>
      // Insert done, verify RowModifying
      match result
      | let r: RowModifying =>
        if r.impacted() != 1 then
          _h.fail(
            "Expected 1 impacted row but got " + r.impacted().string())
          _drop_and_finish()
          return
        end
      else
        _h.fail("Expected RowModifying for INSERT.")
        _drop_and_finish()
        return
      end
      // COMMIT
      _session.execute(SimpleQuery("COMMIT"), this)
    | 4 =>
      // Committed, drop table
      _session.execute(SimpleQuery("DROP TABLE txn_commit_test"), this)
    | 5 =>
      // All done
      _h.complete(true)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _drop_and_finish()
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure at phase " + _phase.string())
    _drop_and_finish()

  fun ref _drop_and_finish() =>
    _session.execute(
      SimpleQuery("DROP TABLE IF EXISTS txn_commit_test"), this)
    _h.complete(false)

  be dispose() =>
    _session.close()

class \nodoc\ iso _TestTransactionRollbackAfterFailure is UnitTest
  """
  Verifies that after an error inside an explicit transaction (status 'E'),
  a ROLLBACK succeeds and the session returns to idle. This exercises the
  bug fix where failed-transaction ReadyForQuery responses previously
  stalled the query queue.
  """
  fun name(): String =>
    "integration/Transaction/RollbackAfterFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _TransactionRollbackClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _TransactionRollbackClient is
  ( SessionStatusNotify
  & ResultReceiver )
  let _h: TestHelper
  let _session: Session
  var _phase: USize = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    // Phase 0: BEGIN
    _phase = 0
    session.execute(SimpleQuery("BEGIN"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // BEGIN succeeded, send invalid query to trigger error state
      _session.execute(
        SimpleQuery("SELECT * FROM this_table_does_not_exist_txn"), this)
    | 3 =>
      // ROLLBACK succeeded, verify we can still query
      _session.execute(SimpleQuery("SELECT 1::text"), this)
    | 4 =>
      // Post-rollback query succeeded
      _h.complete(true)
    else
      _h.fail("Unexpected phase " + _phase.string())
      _h.complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _phase = _phase + 1

    match _phase
    | 2 =>
      // Expected failure from invalid query, now ROLLBACK
      _session.execute(SimpleQuery("ROLLBACK"), this)
    else
      _h.fail("Unexpected query failure at phase " + _phase.string())
      _h.complete(false)
    end

  be dispose() =>
    _session.close()
