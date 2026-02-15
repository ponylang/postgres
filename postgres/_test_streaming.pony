use "collections"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestStreamingSuccess is UnitTest
  """
  Verifies the complete streaming success path: authenticate, send a
  streaming query, receive two batches via PortalSuspended, then a final
  batch via CommandComplete. Verify pg_stream_batch x3 + pg_stream_complete.
  """
  fun name(): String =>
    "Streaming/Success"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7702"

    let listener = _StreamingSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingSuccessTestClient is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper
  var _batches: USize = 0
  var _total_rows: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.stream(
      PreparedQuery("SELECT id FROM t", recover val Array[(String | None)] end),
      2, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _batches = _batches + 1
    _total_rows = _total_rows + rows.size()
    if _batches <= 2 then
      session.fetch_more()
    end
    // Third batch arrives from CommandComplete — no fetch_more needed.

  be pg_stream_complete(session: Session) =>
    if (_batches == 3) and (_total_rows == 5) then
      _close_and_complete(true)
    else
      _h.fail("Expected 3 batches with 5 total rows but got "
        + _batches.string() + " batches with " + _total_rows.string()
        + " rows")
      _close_and_complete(false)
    end

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected stream failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _StreamingSuccessTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _StreamingSuccessTestServer =>
    _StreamingSuccessTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _StreamingSuccessTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _StreamingSuccessTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds to an extended query pipeline
  with RowDescription, then simulates streaming: first Execute returns
  2 DataRows + PortalSuspended, second Execute returns 2 DataRows +
  PortalSuspended, third Execute returns 1 DataRow + CommandComplete +
  ReadyForQuery (after Sync).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  var _execute_count: U8 = 0
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
      // Startup message
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
      // Read all extended query pipeline messages: Parse+Bind+Describe+Execute+Flush
      // We read them one at a time until we get Execute ('E')
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'E' then
            // First Execute received. Send RowDescription + 2 DataRows +
            // PortalSuspended
            _execute_count = _execute_count + 1
            try
              let columns: Array[(String, String)] val = recover val
                [("id", "int4")]
              end
              let row_desc =
                _IncomingRowDescriptionTestMessage(columns)?.bytes()
              _tcp_connection.send(row_desc)
            end
            _send_data_rows(2, (_execute_count.usize() - 1) * 2)
            let ps = _IncomingPortalSuspendedTestMessage.bytes()
            _tcp_connection.send(ps)
            _state = 2
          else
            // Parse, Bind, Describe, Flush — consume and continue
            _process()
          end
        end
      end
    elseif _state == 2 then
      // Subsequent Execute+Flush messages
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'E' then
            _execute_count = _execute_count + 1
            if _execute_count == 3 then
              // Final batch: 1 DataRow + CommandComplete
              _send_data_rows(1, (_execute_count.usize() - 1) * 2)
              let cmd_complete =
                _IncomingCommandCompleteTestMessage("SELECT 5").bytes()
              _tcp_connection.send(cmd_complete)
              // Don't send ReadyForQuery yet — wait for Sync
              _state = 3
            else
              // 2 DataRows + PortalSuspended
              _send_data_rows(2, (_execute_count.usize() - 1) * 2)
              let ps = _IncomingPortalSuspendedTestMessage.bytes()
              _tcp_connection.send(ps)
            end
          else
            // Flush — consume and continue
            _process()
          end
        end
      end
    elseif _state == 3 then
      // Wait for Sync — skip non-Sync messages (e.g. leftover Flush)
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(ready)
          else
            _process()
          end
        end
      end
    end

  fun ref _send_data_rows(count: USize, start: USize) =>
    for i in Range(0, count) do
      let data_row_cols: Array[(String | None)] val = recover val
        let v: String = (start + i + 1).string()
        [as (String | None): v]
      end
      let data_row = _IncomingDataRowTestMessage(data_row_cols).bytes()
      _tcp_connection.send(data_row)
    end

class \nodoc\ iso _TestStreamingEmpty is UnitTest
  """
  Verifies that streaming a query returning zero rows delivers
  pg_stream_complete without any pg_stream_batch calls.
  """
  fun name(): String =>
    "Streaming/Empty"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7703"

    let listener = _StreamingEmptyTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingEmptyTestClient is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper
  var _batches: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.stream(
      PreparedQuery("SELECT id FROM empty", recover val Array[(String | None)] end),
      2, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _batches = _batches + 1

  be pg_stream_complete(session: Session) =>
    if _batches == 0 then
      _close_and_complete(true)
    else
      _h.fail("Expected 0 batches but got " + _batches.string())
      _close_and_complete(false)
    end

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected stream failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _StreamingEmptyTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _StreamingEmptyTestServer =>
    _StreamingEmptyTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _StreamingEmptyTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _StreamingEmptyTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that responds to a streaming query with RowDescription +
  CommandComplete("SELECT 0") — zero rows, no PortalSuspended.
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
          if msg(0)? == 'E' then
            // Execute received — send RowDescription + CommandComplete
            try
              let columns: Array[(String, String)] val = recover val
                [("id", "int4")]
              end
              let row_desc =
                _IncomingRowDescriptionTestMessage(columns)?.bytes()
              let cmd_complete =
                _IncomingCommandCompleteTestMessage("SELECT 0").bytes()
              _tcp_connection.send(row_desc)
              _tcp_connection.send(cmd_complete)
            end
            _state = 2
          else
            _process()
          end
        end
      end
    elseif _state == 2 then
      // Wait for Sync — skip non-Sync messages (e.g. leftover Flush)
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(ready)
          else
            _process()
          end
        end
      end
    end

class \nodoc\ iso _TestStreamingEarlyStop is UnitTest
  """
  Verifies that close_stream() ends streaming early. Client receives one
  batch, calls close_stream() instead of fetch_more(), and verifies
  pg_stream_complete fires.
  """
  fun name(): String =>
    "Streaming/EarlyStop"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7704"

    let listener = _StreamingEarlyStopTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingEarlyStopTestClient is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper
  var _batches: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.stream(
      PreparedQuery("SELECT id FROM t", recover val Array[(String | None)] end),
      2, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _batches = _batches + 1
    // Close instead of fetching more
    session.close_stream()

  be pg_stream_complete(session: Session) =>
    if _batches == 1 then
      _close_and_complete(true)
    else
      _h.fail("Expected 1 batch but got " + _batches.string())
      _close_and_complete(false)
    end

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected stream failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _StreamingEarlyStopTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _StreamingEarlyStopTestServer =>
    _StreamingEarlyStopTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _StreamingEarlyStopTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _StreamingEarlyStopTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds to first Execute with 2 DataRows +
  PortalSuspended, then responds to Sync (from close_stream) with
  ReadyForQuery.
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
          if msg(0)? == 'E' then
            try
              let columns: Array[(String, String)] val = recover val
                [("id", "int4")]
              end
              let row_desc =
                _IncomingRowDescriptionTestMessage(columns)?.bytes()
              _tcp_connection.send(row_desc)
            end
            let data_row_cols: Array[(String | None)] val = recover val
              [as (String | None): "1"]
            end
            let data_row = _IncomingDataRowTestMessage(data_row_cols).bytes()
            _tcp_connection.send(data_row)
            let data_row_cols2: Array[(String | None)] val = recover val
              [as (String | None): "2"]
            end
            let data_row2 = _IncomingDataRowTestMessage(data_row_cols2).bytes()
            _tcp_connection.send(data_row2)
            let ps = _IncomingPortalSuspendedTestMessage.bytes()
            _tcp_connection.send(ps)
            _state = 2
          else
            _process()
          end
        end
      end
    elseif _state == 2 then
      // Wait for Sync (from close_stream) — skip non-Sync messages
      // (e.g. leftover Flush)
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(ready)
          else
            _process()
          end
        end
      end
    end

class \nodoc\ iso _TestStreamingServerError is UnitTest
  """
  Verifies that an ErrorResponse during streaming delivers pg_stream_failed
  and the session remains usable for subsequent queries.
  """
  fun name(): String =>
    "Streaming/ServerError"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7705"

    let listener = _StreamingServerErrorTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingServerErrorTestClient is
  (SessionStatusNotify & StreamingResultReceiver & ResultReceiver)
  let _h: TestHelper
  var _session: (Session | None) = None
  var _stream_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.stream(
      PreparedQuery("SELECT id FROM bad", recover val Array[(String | None)] end),
      2, this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _h.fail("Unexpected stream batch.")
    _close_and_complete(false)

  be pg_stream_complete(session: Session) =>
    _h.fail("Unexpected stream complete.")
    _close_and_complete(false)

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _stream_failed = true
    // Verify session is still usable with a follow-up query
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    if _stream_failed then
      _close_and_complete(true)
    else
      _h.fail("Unexpected query result before stream failure.")
      _close_and_complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Follow-up query failed.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _StreamingServerErrorTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _StreamingServerErrorTestServer =>
    _StreamingServerErrorTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _StreamingServerErrorTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _StreamingServerErrorTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds to the streaming query pipeline
  with ErrorResponse (before any data), then responds to the follow-up
  simple query normally.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  var _error_sent: Bool = false
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
      // Read pipeline messages until we get Execute
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'E' then
            // Send ErrorResponse immediately
            let err = _IncomingErrorResponseTestMessage(
              "ERROR", "42P01", "relation does not exist").bytes()
            _tcp_connection.send(err)
            _error_sent = true
            _state = 2
          else
            _process()
          end
        end
      end
    elseif _state == 2 then
      // Wait for Sync from on_error_response, respond with ReadyForQuery
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'S' then
            let ready = _IncomingReadyForQueryTestMessage('I').bytes()
            _tcp_connection.send(ready)
            _state = 3
          end
        end
        _process()
      end
    elseif _state == 3 then
      // Follow-up simple query
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          if msg(0)? == 'Q' then
            try
              let columns: Array[(String, String)] val = recover val
                [("?column?", "text")]
              end
              let row_desc =
                _IncomingRowDescriptionTestMessage(columns)?.bytes()
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
            end
          end
        end
      end
    end

class \nodoc\ iso _TestStreamingShutdownDrainsQueue is UnitTest
  """
  Verifies that when a session shuts down, pending stream() calls receive
  pg_stream_failed with SessionClosed. Uses a misbehaving server that
  authenticates but never sends ReadyForQuery.
  """
  fun name(): String =>
    "Streaming/ShutdownDrainsQueue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7706"

    let listener = _StreamingShutdownTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingShutdownTestClient is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper
  var _pending: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _pending = 2
    session.stream(
      PreparedQuery("SELECT 1", recover val Array[(String | None)] end),
      2, this)
    session.stream(
      PreparedQuery("SELECT 2", recover val Array[(String | None)] end),
      2, this)
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _h.fail("Unexpected stream batch.")
    _h.complete(false)

  be pg_stream_complete(session: Session) =>
    _h.fail("Unexpected stream complete.")
    _h.complete(false)

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | SessionClosed =>
      _pending = _pending - 1
      if _pending == 0 then
        _h.complete(true)
      end
    else
      _h.fail("Got an incorrect stream failure reason.")
      _h.complete(false)
    end

actor \nodoc\ _StreamingShutdownTestListener is lori.TCPListenerActor
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
    _DoesntAnswerTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _StreamingShutdownTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestStreamingQueryResults is UnitTest
  """
  Integration test: create table, insert 5 rows, stream with window_size=2.
  Verify 3 batches (2+2+1 rows), all rows received, pg_stream_complete.
  """
  fun name(): String =>
    "integration/Streaming/QueryResults"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _StreamingQueryResultsNotify(h))

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _StreamingQueryResultsNotify is
  (SessionStatusNotify & ResultReceiver & StreamingResultReceiver)
  let _h: TestHelper
  var _phase: USize = 0
  var _batches: USize = 0
  var _total_rows: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    _session = session
    // Phase 0: drop table if exists
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS streaming_test"), this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1
    match _phase
    | 1 =>
      // Table dropped. Create it.
      session.execute(
        SimpleQuery("CREATE TABLE streaming_test (id INT NOT NULL)"), this)
    | 2 =>
      // Table created. Insert 5 rows.
      session.execute(
        SimpleQuery(
          "INSERT INTO streaming_test VALUES (1),(2),(3),(4),(5)"), this)
    | 3 =>
      // Rows inserted. Start streaming.
      session.stream(
        PreparedQuery("SELECT id FROM streaming_test ORDER BY id",
          recover val Array[(String | None)] end),
        2, this)
    | 5 =>
      // Table dropped after streaming. Done.
      _close_and_complete(true)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed.")
    _close_and_complete(false)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _batches = _batches + 1
    _total_rows = _total_rows + rows.size()
    session.fetch_more()

  be pg_stream_complete(session: Session) =>
    if (_batches == 3) and (_total_rows == 5) then
      // Drop the table (phase becomes 4, then pg_query_result gets phase 5)
      _phase = _phase + 1
      session.execute(
        SimpleQuery("DROP TABLE streaming_test"), this)
    else
      _h.fail("Expected 3 batches with 5 total rows but got "
        + _batches.string() + " batches with " + _total_rows.string()
        + " rows")
      _close_and_complete(false)
    end

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected stream failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

class \nodoc\ iso _TestStreamingAfterSessionClosed is UnitTest
  """
  Verifies that calling stream() after the session has been closed delivers
  pg_stream_failed with SessionClosed.
  """
  fun name(): String =>
    "integration/Streaming/AfterSessionClosed"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _StreamingAfterSessionClosedNotify(h))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _StreamingAfterSessionClosedNotify is
  (SessionStatusNotify & StreamingResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unexpected authentication failure")

  be pg_session_shutdown(session: Session) =>
    session.stream(
      PreparedQuery("SELECT 1", recover val Array[(String | None)] end),
      2, this)

  be pg_stream_batch(session: Session, rows: Rows) =>
    _h.fail("Unexpected stream batch.")
    _h.complete(false)

  be pg_stream_complete(session: Session) =>
    _h.fail("Unexpected stream complete.")
    _h.complete(false)

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if failure is SessionClosed then
      _h.complete(true)
    else
      _h.fail("Expected SessionClosed but got a different failure.")
      _h.complete(false)
    end
