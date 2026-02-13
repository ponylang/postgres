use "buffered"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestCopyInSuccess is UnitTest
  """
  Verifies the complete COPY IN success path: authenticate, send COPY query,
  receive CopyInResponse, send two data chunks via pull callbacks, send
  CopyDone, receive CommandComplete("COPY 2") + ReadyForQuery.
  """
  fun name(): String =>
    "CopyIn/Success"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7688"

    let listener = _CopyInSuccessTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyInSuccessTestClient is
  (SessionStatusNotify & CopyInReceiver)
  let _h: TestHelper
  var _pulls: USize = 0
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.copy_in("COPY t FROM STDIN", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_ready(session: Session) =>
    _pulls = _pulls + 1
    if _pulls <= 2 then
      let row: Array[U8] val = recover val
        "row\tdata\n".array()
      end
      session.send_copy_data(row)
    else
      session.finish_copy()
    end

  be pg_copy_complete(session: Session, count: USize) =>
    if count == 2 then
      _close_and_complete(true)
    else
      _h.fail("Expected count 2 but got " + count.string())
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

actor \nodoc\ _CopyInSuccessTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyInSuccessTestServer =>
    _CopyInSuccessTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyInSuccessTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyInSuccessTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds to a COPY query with
  CopyInResponse, expects CopyData messages followed by CopyDone,
  then sends CommandComplete("COPY 2") + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received_count: USize = 0
  let _readbuf: Reader = _readbuf.create()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup: send AuthOk + ReadyForQuery(idle)
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    elseif _received_count == 2 then
      // COPY query: respond with CopyInResponse (text format, 2 columns)
      let col_fmts: Array[U8] val = recover val [as U8: 0; 0] end
      let copy_in = _IncomingCopyInResponseTestMessage(0, col_fmts).bytes()
      _tcp_connection.send(copy_in)
    else
      // Only buffer data during the COPY phase (type+length format)
      _readbuf.append(consume data)
      _check_for_copy_done()
    end

  fun ref _check_for_copy_done() =>
    // Scan the buffer for a CopyDone message type ('c')
    while _readbuf.size() >= 5 do
      try
        let msg_type = _readbuf.peek_u8(0)?
        let msg_len = _readbuf.peek_u32_be(1)?.usize()
        let total = msg_len + 1
        if _readbuf.size() < total then
          return
        end
        if msg_type == 'c' then
          // CopyDone received — send CommandComplete + ReadyForQuery
          _readbuf.skip(total)?
          let cmd_complete =
            _IncomingCommandCompleteTestMessage("COPY 2").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          _tcp_connection.send(cmd_complete)
          _tcp_connection.send(ready)
          return
        else
          // Skip other messages (CopyData, Query, etc.)
          _readbuf.skip(total)?
        end
      else
        return
      end
    end

class \nodoc\ iso _TestCopyInAbort is UnitTest
  """
  Verifies that aborting a COPY IN operation delivers pg_copy_failed.
  Client sends CopyFail in response to pg_copy_ready. Server responds
  with ErrorResponse + ReadyForQuery.
  """
  fun name(): String =>
    "CopyIn/Abort"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7689"

    let listener = _CopyInAbortTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyInAbortTestClient is
  (SessionStatusNotify & CopyInReceiver)
  let _h: TestHelper
  var _session: (Session | None) = None

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _session = session
    session.copy_in("COPY t FROM STDIN", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_ready(session: Session) =>
    session.abort_copy("client abort")

  be pg_copy_complete(session: Session, count: USize) =>
    _h.fail("Unexpected copy complete.")
    _close_and_complete(false)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _close_and_complete(true)
    else
      _h.fail("Expected ErrorResponseMessage but got ClientQueryError.")
      _close_and_complete(false)
    end

  fun ref _close_and_complete(success: Bool) =>
    match _session
    | let s: Session => s.close()
    end
    _h.complete(success)

actor \nodoc\ _CopyInAbortTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyInAbortTestServer =>
    _CopyInAbortTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyInAbortTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyInAbortTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds with CopyInResponse, then
  responds to CopyFail with ErrorResponse + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received_count: USize = 0
  let _readbuf: Reader = _readbuf.create()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    elseif _received_count == 2 then
      let col_fmts: Array[U8] val = recover val [as U8: 0] end
      let copy_in = _IncomingCopyInResponseTestMessage(0, col_fmts).bytes()
      _tcp_connection.send(copy_in)
    else
      _readbuf.append(consume data)
      _check_for_copy_fail()
    end

  fun ref _check_for_copy_fail() =>
    while _readbuf.size() >= 5 do
      try
        let msg_type = _readbuf.peek_u8(0)?
        let msg_len = _readbuf.peek_u32_be(1)?.usize()
        let total = msg_len + 1
        if _readbuf.size() < total then
          return
        end
        if msg_type == 'f' then
          // CopyFail received — send ErrorResponse + ReadyForQuery
          _readbuf.skip(total)?
          let err = _IncomingErrorResponseTestMessage(
            "ERROR", "57014", "COPY aborted").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          _tcp_connection.send(err)
          _tcp_connection.send(ready)
          return
        else
          _readbuf.skip(total)?
        end
      else
        return
      end
    end

class \nodoc\ iso _TestCopyInServerError is UnitTest
  """
  Verifies that a server error during COPY IN delivers pg_copy_failed and
  the session remains usable (a follow-up query succeeds).
  """
  fun name(): String =>
    "CopyIn/ServerError"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7690"

    let listener = _CopyInServerErrorTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyInServerErrorTestClient is
  (SessionStatusNotify & CopyInReceiver & ResultReceiver)
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
    session.copy_in("COPY t FROM STDIN", this)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_ready(session: Session) =>
    let row: Array[U8] val = recover val
      "data\n".array()
    end
    session.send_copy_data(row)

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

actor \nodoc\ _CopyInServerErrorTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _CopyInServerErrorTestServer =>
    _CopyInServerErrorTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _CopyInServerErrorTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CopyInServerErrorTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, responds with CopyInResponse, then
  responds to CopyData with ErrorResponse + ReadyForQuery (simulating a
  server-side error during COPY). Follow-up queries get normal responses.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received_count: USize = 0
  var _copy_error_sent: Bool = false
  let _readbuf: Reader = _readbuf.create()

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    elseif _received_count == 2 then
      let col_fmts: Array[U8] val = recover val [as U8: 0] end
      let copy_in = _IncomingCopyInResponseTestMessage(0, col_fmts).bytes()
      _tcp_connection.send(copy_in)
    else
      _readbuf.append(consume data)
      _process_messages()
    end

  fun ref _process_messages() =>
    while _readbuf.size() >= 5 do
      try
        let msg_type = _readbuf.peek_u8(0)?
        let msg_len = _readbuf.peek_u32_be(1)?.usize()
        let total = msg_len + 1
        if _readbuf.size() < total then
          return
        end
        _readbuf.skip(total)?

        if (msg_type == 'd') and (not _copy_error_sent) then
          // First CopyData: respond with ErrorResponse + ReadyForQuery
          _copy_error_sent = true
          let err = _IncomingErrorResponseTestMessage(
            "ERROR", "22P04", "invalid input syntax").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          _tcp_connection.send(err)
          _tcp_connection.send(ready)
        elseif msg_type == 'Q' then
          // Follow-up query: respond with CommandComplete + ReadyForQuery
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
      else
        return
      end
    end

class \nodoc\ iso _TestCopyInShutdownDrainsCopyQueue is UnitTest
  """
  Verifies that when a session shuts down, pending copy_in() calls receive
  pg_copy_failed with SessionClosed. Uses a misbehaving server that
  authenticates but never sends ReadyForQuery.
  """
  fun name(): String =>
    "CopyIn/ShutdownDrainsCopyQueue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7691"

    let listener = _CopyInShutdownTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyInShutdownTestClient is
  (SessionStatusNotify & CopyInReceiver)
  let _h: TestHelper
  var _pending: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authenticated(session: Session) =>
    _pending = 2
    session.copy_in("COPY t1 FROM STDIN", this)
    session.copy_in("COPY t2 FROM STDIN", this)
    session.close()

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

  be pg_copy_ready(session: Session) =>
    _h.fail("Unexpected copy ready.")
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

actor \nodoc\ _CopyInShutdownTestListener is lori.TCPListenerActor
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
      _CopyInShutdownTestClient(_h))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

class \nodoc\ iso _TestCopyInAfterSessionClosed is UnitTest
  """
  Verifies that calling copy_in() after the session has been closed delivers
  pg_copy_failed with SessionClosed.
  """
  fun name(): String =>
    "integration/CopyIn/AfterSessionClosed"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      _CopyInAfterSessionClosedNotify(h))

    h.dispose_when_done(session)
    h.long_test(5_000_000_000)

actor \nodoc\ _CopyInAfterSessionClosedNotify is
  (SessionStatusNotify & CopyInReceiver)
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
    session.copy_in("COPY t FROM STDIN", this)

  be pg_copy_ready(session: Session) =>
    _h.fail("Unexpected copy ready.")
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
