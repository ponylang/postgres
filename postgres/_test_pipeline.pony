use "collections"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestPipelineSuccess is UnitTest
  """
  Verifies the complete pipeline success path: authenticate, send a pipeline
  of 3 queries, verify indexed results + pg_pipeline_complete.
  """
  fun name(): String =>
    "Pipeline/Success"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7711"

    let listener = _PipelineSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineSuccessTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 2",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 3",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected pipeline failure at index " + index.string())
    _close_and_complete(false)

  be pg_pipeline_complete(session: Session) =>
    if _results == 3 then
      _close_and_complete(true)
    else
      _h.fail("Expected 3 results but got " + _results.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineSuccessTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineSuccessTestServer =>
    let server = _PipelineSuccessTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineSuccessTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineSuccessTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then responds to 3 pipelined query cycles.
  Each cycle: Parse+Bind+Describe+Execute+Sync from client; server sends
  RowDescription+DataRow+CommandComplete+ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  var _query_count: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      // Read pipeline messages, respond to each Sync with a query result
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            // Sync received — send result for this query cycle
            _query_count = _query_count + 1
            let columns: Array[(String, U32, U16)] val = recover val
              [("?column?", U32(23), U16(0))]
            end
            let row_desc =
              _IncomingRowDescriptionTestMessage(columns).bytes()
            let data_row_cols: Array[(String | None)] val = recover val
              [as (String | None): _query_count.string()]
            end
            let data_row =
              _IncomingDataRowTestMessage(data_row_cols).bytes()
            let cmd_complete =
              _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(row_desc)
            _tcp_connection.send(data_row)
            _tcp_connection.send(cmd_complete)
            _tcp_connection.send(ready)
          end
        end
        _process()
      end
    end

class \nodoc\ iso _TestPipelineWithFailure is UnitTest
  """
  Verifies error isolation: middle query fails, first and last succeed.
  """
  fun name(): String =>
    "Pipeline/WithFailure"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7712"

    let listener = _PipelineWithFailureTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineWithFailureTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _failures: USize = 0
  var _failed_index: USize = USize.max_value()
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT bad_table",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 3",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _failures = _failures + 1
    _failed_index = index

  be pg_pipeline_complete(session: Session) =>
    if (_results == 2) and (_failures == 1) and (_failed_index == 1) then
      _close_and_complete(true)
    else
      _h.fail("Expected 2 results, 1 failure at index 1; got "
        + _results.string() + " results, " + _failures.string()
        + " failures, failed_index=" + _failed_index.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineWithFailureTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineWithFailureTestServer =>
    let server = _PipelineWithFailureTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineWithFailureTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineWithFailureTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then processes 3 pipelined query cycles.
  Query 1 succeeds, query 2 returns ErrorResponse, query 3 succeeds.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  var _sync_count: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            _sync_count = _sync_count + 1
            if _sync_count == 2 then
              // Query 2 fails
              let err = _IncomingErrorResponseTestMessage(
                "ERROR", "42P01", "relation does not exist").bytes()
              let ready = _IncomingReadyForQueryTestMessage('I').bytes()
              _tcp_connection.send(err)
              _tcp_connection.send(ready)
            else
              // Queries 1 and 3 succeed
              let columns: Array[(String, U32, U16)] val = recover val
                [("?column?", U32(23), U16(0))]
              end
              let row_desc =
                _IncomingRowDescriptionTestMessage(columns).bytes()
              let data_row_cols: Array[(String | None)] val = recover val
                [as (String | None): _sync_count.string()]
              end
              let data_row =
                _IncomingDataRowTestMessage(data_row_cols).bytes()
              let cmd_complete =
                _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
              let ready = _IncomingReadyForQueryTestMessage('I').bytes()
              _tcp_connection.send(row_desc)
              _tcp_connection.send(data_row)
              _tcp_connection.send(cmd_complete)
              _tcp_connection.send(ready)
            end
          end
        end
        _process()
      end
    end

class \nodoc\ iso _TestPipelineEmpty is UnitTest
  """
  Verifies that an empty pipeline delivers pg_pipeline_complete immediately.
  """
  fun name(): String =>
    "Pipeline/Empty"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7713"

    let listener = _PipelineEmptyTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineEmptyTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _failures: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      Array[(PreparedQuery | NamedPreparedQuery)]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _failures = _failures + 1

  be pg_pipeline_complete(session: Session) =>
    if (_results == 0) and (_failures == 0) then
      _close_and_complete(true)
    else
      _h.fail("Expected 0 results and 0 failures but got "
        + _results.string() + " results, " + _failures.string()
        + " failures")
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineEmptyTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineEmptyTestServer =>
    let server = _PipelineEmptyTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineEmptyTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineEmptyTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates and sends ReadyForQuery. The empty pipeline
  dispatches without sending any messages.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    match _reader.read_startup_message()
    | let _: Array[U8] val =>
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    end

class \nodoc\ iso _TestPipelineSingleQuery is UnitTest
  """
  Verifies a degenerate 1-query pipeline works correctly.
  """
  fun name(): String =>
    "Pipeline/SingleQuery"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7714"

    let listener = _PipelineSingleQueryTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineSingleQueryTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 42",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1
    if index != 0 then
      _h.fail("Expected index 0 but got " + index.string())
    end

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected pipeline failure.")
    _close_and_complete(false)

  be pg_pipeline_complete(session: Session) =>
    if _results == 1 then
      _close_and_complete(true)
    else
      _h.fail("Expected 1 result but got " + _results.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineSingleQueryTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineSuccessTestServer =>
    let server = _PipelineSuccessTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineSingleQueryTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestPipelineShutdownDrainsQueue is UnitTest
  """
  Verifies that when a session shuts down, pending pipeline() calls receive
  pg_pipeline_failed with SessionClosed for all queries + pg_pipeline_complete.
  Uses a misbehaving server that authenticates but never sends ReadyForQuery.
  """
  fun name(): String =>
    "Pipeline/ShutdownDrainsQueue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7715"

    let listener = _PipelineShutdownDrainsQueueTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineShutdownDrainsQueueTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _failures: USize = 0
  var _completed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 2",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)
    session.close()

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _h.fail("Unexpected pipeline result.")
    _h.complete(false)

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | SessionClosed =>
      _failures = _failures + 1
    else
      _h.fail("Expected SessionClosed failure.")
      _h.complete(false)
    end

  be pg_pipeline_complete(session: Session) =>
    if _failures == 2 then
      _h.complete(true)
    else
      _h.fail("Expected 2 failures but got " + _failures.string())
      _h.complete(false)
    end

actor \nodoc\ _PipelineShutdownDrainsQueueTestListener is lori.TCPListenerActor
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
    let server = _DoesntAnswerTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineShutdownDrainsQueueTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestPipelineShutdownInFlight is UnitTest
  """
  Verifies that close() while a pipeline is actively executing drains all
  remaining queries with pg_pipeline_failed(SessionClosed) and delivers
  pg_pipeline_complete.
  """
  fun name(): String =>
    "Pipeline/ShutdownInFlight"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7716"

    let listener = _PipelineShutdownInFlightTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineShutdownInFlightTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _failures: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 2",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 3",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    // First result arrives, then close
    match _session
    | let s: Session => s.close()
    end

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | SessionClosed =>
      _failures = _failures + 1
    else
      _h.fail("Expected SessionClosed failure.")
      _h.complete(false)
    end

  be pg_pipeline_complete(session: Session) =>
    // We expect at least some failures from shutdown drain
    if _failures > 0 then
      _h.complete(true)
    else
      _h.fail("Expected at least 1 SessionClosed failure.")
      _h.complete(false)
    end

actor \nodoc\ _PipelineShutdownInFlightTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineShutdownInFlightTestServer =>
    let server = _PipelineShutdownInFlightTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineShutdownInFlightTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineShutdownInFlightTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates + sends ReadyForQuery so the pipeline enters
  _PipelineInFlight, then responds to only the first Sync (so close() fires
  while the pipeline is partially complete).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  var _sync_sent: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if (msg(0)? == 'S') and (not _sync_sent) then
            // Respond to only the first Sync
            _sync_sent = true
            let columns: Array[(String, U32, U16)] val = recover val
              [("?column?", U32(23), U16(0))]
            end
            let row_desc =
              _IncomingRowDescriptionTestMessage(columns).bytes()
            let data_row_cols: Array[(String | None)] val = recover val
              [as (String | None): "1"]
            end
            let data_row =
              _IncomingDataRowTestMessage(data_row_cols).bytes()
            let cmd_complete =
              _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(row_desc)
            _tcp_connection.send(data_row)
            _tcp_connection.send(cmd_complete)
            _tcp_connection.send(ready)
            // After this, go silent — client will call close()
          end
        end
        _process()
      end
    end

class \nodoc\ iso _TestPipelineRowModifying is UnitTest
  """
  Verifies pipeline with INSERT queries that return no rows.
  """
  fun name(): String =>
    "Pipeline/RowModifying"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7717"

    let listener = _PipelineRowModifyingTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineRowModifyingTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("INSERT INTO t VALUES (1)",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("INSERT INTO t VALUES (2)",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    match result
    | let rm: RowModifying =>
      _results = _results + 1
    else
      _h.fail("Expected RowModifying result.")
      _close_and_complete(false)
    end

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected pipeline failure.")
    _close_and_complete(false)

  be pg_pipeline_complete(session: Session) =>
    if _results == 2 then
      _close_and_complete(true)
    else
      _h.fail("Expected 2 results but got " + _results.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineRowModifyingTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineRowModifyingTestServer =>
    let server = _PipelineRowModifyingTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineRowModifyingTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineRowModifyingTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then responds to 2 pipelined INSERT queries
  with CommandComplete("INSERT 0 1") and ReadyForQuery (no RowDescription).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let cmd_complete =
              _IncomingCommandCompleteTestMessage("INSERT 0 1").bytes()
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(cmd_complete)
            _tcp_connection.send(ready)
          end
        end
        _process()
      end
    end

class \nodoc\ iso _TestPipelineMixedQueryTypes is UnitTest
  """
  Verifies a pipeline with mixed PreparedQuery + NamedPreparedQuery.
  """
  fun name(): String =>
    "Pipeline/MixedQueryTypes"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7718"

    let listener = _PipelineMixedQueryTypesTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineMixedQueryTypesTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        NamedPreparedQuery("my_stmt",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected pipeline failure at index " + index.string())
    _close_and_complete(false)

  be pg_pipeline_complete(session: Session) =>
    if _results == 2 then
      _close_and_complete(true)
    else
      _h.fail("Expected 2 results but got " + _results.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineMixedQueryTypesTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineSuccessTestServer =>
    let server = _PipelineSuccessTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineMixedQueryTypesTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestPipelineAllFail is UnitTest
  """
  Verifies that all queries in a pipeline can fail and each gets its own
  pg_pipeline_failed + pg_pipeline_complete fires.
  """
  fun name(): String =>
    "Pipeline/AllFail"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7719"

    let listener = _PipelineAllFailTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _PipelineAllFailTestClient is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _failures: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT bad1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT bad2",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _h.fail("Unexpected pipeline result.")
    _close_and_complete(false)

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _failures = _failures + 1

  be pg_pipeline_complete(session: Session) =>
    if _failures == 2 then
      _close_and_complete(true)
    else
      _h.fail("Expected 2 failures but got " + _failures.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _PipelineAllFailTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _PipelineAllFailTestServer =>
    let server = _PipelineAllFailTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _PipelineAllFailTestClient(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _PipelineAllFailTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then responds to all pipelined Syncs with
  ErrorResponse + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let err = _IncomingErrorResponseTestMessage(
              "ERROR", "42P01", "relation does not exist").bytes()
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(err)
            _tcp_connection.send(ready)
          end
        end
        _process()
      end
    end

class \nodoc\ iso _TestPipelineIntegration is UnitTest
  """
  Integration test: pipeline 3 SELECTs against real PostgreSQL.
  """
  fun name(): String =>
    "integration/Pipeline/QueryResults"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _PipelineIntegrationNotify(h))

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _PipelineIntegrationNotify is
  (SessionStatusNotify & ResultReceiver & PipelineReceiver)
  let _h: TestHelper
  var _phase: USize = 0
  var _results: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    _session = session
    // Phase 0: drop table if exists
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS pipeline_test"), this)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Connection failed before reaching authenticated state.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1
    match _phase
    | 1 =>
      // Table dropped. Create it.
      session.execute(
        SimpleQuery(
          "CREATE TABLE pipeline_test (id INT NOT NULL, name TEXT NOT NULL)"),
        this)
    | 2 =>
      // Table created. Insert rows.
      session.execute(
        SimpleQuery(
          "INSERT INTO pipeline_test VALUES (1,'a'),(2,'b'),(3,'c')"),
        this)
    | 3 =>
      // Rows inserted. Pipeline 3 SELECTs.
      let queries = recover val
        [as (PreparedQuery | NamedPreparedQuery):
          PreparedQuery("SELECT id, name FROM pipeline_test WHERE id = $1",
            recover val [as FieldDataTypes: I32(1)] end)
          PreparedQuery("SELECT id, name FROM pipeline_test WHERE id = $1",
            recover val [as FieldDataTypes: I32(2)] end)
          PreparedQuery("SELECT id, name FROM pipeline_test WHERE id = $1",
            recover val [as FieldDataTypes: I32(3)] end)
        ]
      end
      session.pipeline(queries, this)
    | 5 =>
      // Table dropped after pipeline. Done.
      _close_and_complete(true)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed.")
    _close_and_complete(false)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Pipeline query failed at index " + index.string())
    _close_and_complete(false)

  be pg_pipeline_complete(session: Session) =>
    if _results == 3 then
      _phase = _phase + 1
      session.execute(
        SimpleQuery("DROP TABLE pipeline_test"), this)
    else
      _h.fail("Expected 3 pipeline results but got " + _results.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

class \nodoc\ iso _TestPipelineIntegrationWithFailure is UnitTest
  """
  Integration test: pipeline with one bad query, verify error isolation.
  """
  fun name(): String =>
    "integration/Pipeline/WithFailure"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _PipelineIntegrationWithFailureNotify(h))

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _PipelineIntegrationWithFailureNotify is
  (SessionStatusNotify & PipelineReceiver)
  let _h: TestHelper
  var _results: USize = 0
  var _failures: USize = 0
  var _failed_index: USize = USize.max_value()
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    _session = session
    let queries = recover val
      [as (PreparedQuery | NamedPreparedQuery):
        PreparedQuery("SELECT 1",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT * FROM nonexistent_table_xyz",
          recover val Array[FieldDataTypes] end)
        PreparedQuery("SELECT 3",
          recover val Array[FieldDataTypes] end)
      ]
    end
    session.pipeline(queries, this)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Connection failed before reaching authenticated state.")
    _h.complete(false)

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _results = _results + 1

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _failures = _failures + 1
    _failed_index = index

  be pg_pipeline_complete(session: Session) =>
    if (_results == 2) and (_failures == 1) and (_failed_index == 1) then
      _close_and_complete(true)
    else
      _h.fail("Expected 2 results, 1 failure at index 1; got "
        + _results.string() + " results, " + _failures.string()
        + " failures, failed_index=" + _failed_index.string())
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)
