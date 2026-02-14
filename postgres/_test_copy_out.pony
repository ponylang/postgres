use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestCopyOutSuccess is UnitTest
  """
  Verifies the complete COPY OUT success path: authenticate, send COPY query,
  receive CopyOutResponse, receive two CopyData chunks, receive CopyDone,
  receive CommandComplete("COPY 2") + ReadyForQuery.
  """
  fun name(): String =>
    "CopyOut/Success"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7698"

    let listener = _CopyOutSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyOutSuccessTestClient is
  (SessionStatusNotify & CopyOutReceiver)
  let _h: TestHelper
  var _chunks: Array[Array[U8] val] = _chunks.create()
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.copy_out("COPY t TO STDOUT", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _chunks.push(data)

  be pg_copy_complete(session: Session, count: USize) =>
    if (count == 2) and (_chunks.size() == 2) then
      _close_and_complete(true)
    else
      _h.fail("Expected count 2 and 2 chunks but got "
        + count.string() + " and " + _chunks.size().string())
      _close_and_complete(false)
    end

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected copy failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _CopyOutSuccessTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyOutSuccessTestServer =>
    _CopyOutSuccessTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyOutSuccessTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyOutSuccessTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds to a COPY query with
  CopyOutResponse, sends two CopyData messages, then CopyDone +
  CommandComplete("COPY 2") + ReadyForQuery.
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
      | let _: Array[U8] val =>
        let col_fmts: Array[U8] val = recover val [as U8: 0; 0] end
        let copy_out =
          _IncomingCopyOutResponseTestMessage(0, col_fmts).bytes()
        let data1 = _IncomingCopyDataTestMessage(
          "row1\tval1\n".array()).bytes()
        let data2 = _IncomingCopyDataTestMessage(
          "row2\tval2\n".array()).bytes()
        let copy_done = _IncomingCopyDoneTestMessage.bytes()
        let cmd_complete =
          _IncomingCommandCompleteTestMessage("COPY 2").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(copy_out)
        _tcp_connection.send(data1)
        _tcp_connection.send(data2)
        _tcp_connection.send(copy_done)
        _tcp_connection.send(cmd_complete)
        _tcp_connection.send(ready)
        _state = 2
      end
    end

class \nodoc\ iso _TestCopyOutEmpty is UnitTest
  """
  Verifies COPY OUT with zero rows: CopyOutResponse, CopyDone,
  CommandComplete("COPY 0"), ReadyForQuery. No pg_copy_data calls should
  occur.
  """
  fun name(): String =>
    "CopyOut/Empty"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7699"

    let listener = _CopyOutEmptyTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyOutEmptyTestClient is
  (SessionStatusNotify & CopyOutReceiver)
  let _h: TestHelper
  var _data_received: Bool = false
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.copy_out("COPY t TO STDOUT", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _data_received = true

  be pg_copy_complete(session: Session, count: USize) =>
    if (count == 0) and (not _data_received) then
      _close_and_complete(true)
    else
      _h.fail("Expected count 0 and no data but got "
        + count.string())
      _close_and_complete(false)
    end

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected copy failure.")
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _CopyOutEmptyTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyOutEmptyTestServer =>
    _CopyOutEmptyTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyOutEmptyTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyOutEmptyTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds with CopyOutResponse, then
  immediately sends CopyDone + CommandComplete("COPY 0") + ReadyForQuery
  (zero rows exported).
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
      | let _: Array[U8] val =>
        let col_fmts: Array[U8] val = recover val [as U8: 0] end
        let copy_out =
          _IncomingCopyOutResponseTestMessage(0, col_fmts).bytes()
        let copy_done = _IncomingCopyDoneTestMessage.bytes()
        let cmd_complete =
          _IncomingCommandCompleteTestMessage("COPY 0").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(copy_out)
        _tcp_connection.send(copy_done)
        _tcp_connection.send(cmd_complete)
        _tcp_connection.send(ready)
        _state = 2
      end
    end

class \nodoc\ iso _TestCopyOutServerError is UnitTest
  """
  Verifies that a server error during COPY OUT delivers pg_copy_failed and
  the session remains usable (a follow-up query succeeds).
  """
  fun name(): String =>
    "CopyOut/ServerError"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7700"

    let listener = _CopyOutServerErrorTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyOutServerErrorTestClient is
  (SessionStatusNotify & CopyOutReceiver & ResultReceiver)
  let _h: TestHelper
  var _session: (Session | None) = None
  var _copy_failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.copy_out("COPY t TO STDOUT", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    None

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Unexpected copy complete.")
    _close_and_complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _copy_failed = true
    // After failure, verify session is still usable with a follow-up query
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    if _copy_failed then
      _close_and_complete(true)
    else
      _h.fail("Unexpected query result before copy failure.")
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

actor \nodoc\ _CopyOutServerErrorTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyOutServerErrorTestServer =>
    _CopyOutServerErrorTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyOutServerErrorTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyOutServerErrorTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds with CopyOutResponse, sends one
  CopyData, then ErrorResponse + ReadyForQuery (simulating a server-side
  error during COPY). Follow-up queries get normal responses.
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
      | let _: Array[U8] val =>
        let col_fmts: Array[U8] val = recover val [as U8: 0] end
        let copy_out =
          _IncomingCopyOutResponseTestMessage(0, col_fmts).bytes()
        let data1 = _IncomingCopyDataTestMessage(
          "row1\tval1\n".array()).bytes()
        let err = _IncomingErrorResponseTestMessage(
          "ERROR", "XX000", "copy out error").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(copy_out)
        _tcp_connection.send(data1)
        _tcp_connection.send(err)
        _tcp_connection.send(ready)
        _state = 2
        _process()
      end
    else
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
        _process()
      end
    end

class \nodoc\ iso _TestCopyOutShutdownDrainsCopyQueue is UnitTest
  """
  Verifies that when a session shuts down, pending copy_out() calls receive
  pg_copy_failed with SessionClosed. Uses a misbehaving server that
  authenticates but never sends ReadyForQuery.
  """
  fun name(): String =>
    "CopyOut/ShutdownDrainsCopyQueue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7701"

    let listener = _CopyOutShutdownTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyOutShutdownTestClient is
  (SessionStatusNotify & CopyOutReceiver)
  let _h: TestHelper
  var _pending: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _pending = 2
    session.copy_out("COPY t1 TO STDOUT", this)
    session.copy_out("COPY t2 TO STDOUT", this)
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _h.fail("Unexpected copy data.")
    _h.complete(false)

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Unexpected copy complete.")
    _h.complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | SessionClosed =>
      _pending = _pending - 1
      if _pending == 0 then
        _h.complete(true)
      end
    else
      _h.fail("Got an incorrect copy failure reason.")
      _h.complete(false)
    end

actor \nodoc\ _CopyOutShutdownTestListener is lori.TCPListenerActor
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
      _CopyOutShutdownTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestCopyOutAfterSessionClosed is UnitTest
  """
  Verifies that calling copy_out() after the session has been closed delivers
  pg_copy_failed with SessionClosed.
  """
  fun name(): String =>
    "integration/CopyOut/AfterSessionClosed"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _CopyOutAfterSessionClosedNotify(h))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyOutAfterSessionClosedNotify is
  (SessionStatusNotify & CopyOutReceiver)
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
    session.copy_out("COPY t TO STDOUT", this)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _h.fail("Unexpected copy data.")
    _h.complete(false)

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Unexpected copy complete.")
    _h.complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    if failure is SessionClosed then
      _h.complete(true)
    else
      _h.fail("Expected SessionClosed but got a different failure.")
      _h.complete(false)
    end

class \nodoc\ iso _TestCopyOutExport is UnitTest
  """
  Integration test: create a table, insert rows, COPY TO STDOUT, verify
  the received data contains the expected rows, then drop the table.
  """
  fun name(): String =>
    "integration/CopyOut/Export"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _CopyOutExportTestClient(h))

    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

actor \nodoc\ _CopyOutExportTestClient is
  (SessionStatusNotify & ResultReceiver & CopyOutReceiver)
  let _h: TestHelper
  var _phase: USize = 0
  var _copy_data: Array[U8] iso = recover iso Array[U8] end
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    _phase = 0
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS copy_out_test"), this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _copy_data.append(data)

  be pg_copy_complete(session: Session, count: USize) =>
    if count != 3 then
      _h.fail("Expected COPY count 3 but got " + count.string())
      _close_and_complete(false)
      return
    end
    // Verify the data contains 3 lines
    let received: String val = String.from_iso_array(
      _copy_data = recover iso Array[U8] end)
    let lines: Array[String] val = received.split("\n")
    // split produces a trailing empty string after the final newline
    var non_empty: USize = 0
    for line in lines.values() do
      if line.size() > 0 then
        non_empty = non_empty + 1
      end
    end
    if non_empty == 3 then
      // Drop the table
      session.execute(
        SimpleQuery("DROP TABLE copy_out_test"), this)
    else
      _h.fail("Expected 3 non-empty lines but got " + non_empty.string())
      _close_and_complete(false)
    end

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected copy failure.")
    _close_and_complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table dropped (or didn't exist). Create it.
      session.execute(
        SimpleQuery(
          "CREATE TABLE copy_out_test (id INT NOT NULL, name TEXT NOT NULL)"),
        this)
    | 2 =>
      // Table created. Insert 3 rows.
      session.execute(
        SimpleQuery(
          "INSERT INTO copy_out_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"),
        this)
    | 3 =>
      // Rows inserted. Start COPY OUT.
      session.copy_out(
        "COPY copy_out_test TO STDOUT", this)
    | 4 =>
      // Table dropped after successful COPY. Done.
      _close_and_complete(true)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _h.fail("Query failed: " + e.code + ": " + e.message)
    | let e: ClientQueryError =>
      _h.fail("Query failed: client error")
    end
    _close_and_complete(false)

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)
