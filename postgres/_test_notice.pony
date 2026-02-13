use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestNoticeDelivery is UnitTest
  """
  Verifies that pg_notice fires with the correct NoticeResponseMessage fields
  when the server sends a NoticeResponse between CommandComplete and
  ReadyForQuery.
  """
  fun name(): String =>
    "Notice/Delivery"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7692"

    let listener = _NoticeTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _NoticeDeliveryClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _NoticeDeliveryClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_result: Bool = false
  var _got_notice: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_notice(session: Session, notice: NoticeResponseMessage) =>
    _got_notice = true
    if notice.severity != "NOTICE" then
      _h.fail("Expected severity 'NOTICE' but got '" +
        notice.severity + "'")
      session.close()
      _h.complete(false)
      return
    end
    if notice.code != "00000" then
      _h.fail("Expected code '00000' but got '" +
        notice.code + "'")
      session.close()
      _h.complete(false)
      return
    end
    if notice.message != "test notice" then
      _h.fail("Expected message 'test notice' but got '" +
        notice.message + "'")
      session.close()
      _h.complete(false)
      return
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and notice.
    if _got_result then
      if not _got_notice then
        _h.fail("Got query result but no notice.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

class \nodoc\ iso _TestNoticeDuringDataRows is UnitTest
  """
  Verifies that a NoticeResponse arriving between DataRow messages
  still delivers both the complete query result (with all data rows) and the
  notice.
  """
  fun name(): String =>
    "Notice/DuringDataRows"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7693"

    let listener = _NoticeMidQueryTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _NoticeDuringDataRowsClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _NoticeDuringDataRowsClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_notice: Bool = false
  var _got_result: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("SELECT 1"), this)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true
    match result
    | let r: ResultSet =>
      if r.rows().size() != 2 then
        _h.fail("Expected 2 rows but got " + r.rows().size().string())
        session.close()
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet.")
      session.close()
      _h.complete(false)
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_notice(session: Session, notice: NoticeResponseMessage) =>
    _got_notice = true
    if notice.severity != "NOTICE" then
      _h.fail("Expected severity 'NOTICE' but got '" +
        notice.severity + "'")
      session.close()
      _h.complete(false)
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and notice.
    if _got_result then
      if not _got_notice then
        _h.fail("Got query result but no notice.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

// Shared mock server infrastructure for notice tests

actor \nodoc\ _NoticeTestListener is lori.TCPListenerActor
  """
  Mock server that authenticates, waits for a query, then responds with
  CommandComplete + NoticeResponse + ReadyForQuery.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _notify: (SessionStatusNotify & ResultReceiver)

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    notify: (SessionStatusNotify & ResultReceiver),
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _notify = notify
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _NoticeTestServer =>
    _NoticeTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _NoticeTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  CommandComplete + NoticeResponse + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _authed then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _authed = true
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _process()
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        let cmd = _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
        let notice = _IncomingNoticeResponseTestMessage(
          "NOTICE", "00000", "test notice").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(cmd)
        _tcp_connection.send(notice)
        _tcp_connection.send(ready)
      end
    end

actor \nodoc\ _NoticeMidQueryTestListener is lori.TCPListenerActor
  """
  Mock server listener for the mid-query notice test.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _notify: (SessionStatusNotify & ResultReceiver)

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    notify: (SessionStatusNotify & ResultReceiver),
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _notify = notify
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _NoticeMidQueryTestServer =>
    _NoticeMidQueryTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _NoticeMidQueryTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  RowDescription + DataRow + NoticeResponse + DataRow +
  CommandComplete + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _authed then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _authed = true
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        _tcp_connection.send(auth_ok)
        _tcp_connection.send(ready)
        _process()
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        try
          let columns: Array[(String, String)] val = [("col", "text")]
          let row_desc =
            _IncomingRowDescriptionTestMessage(columns)?.bytes()

          let row1_cols: Array[(String | None)] val = ["row1"]
          let data_row_1 = _IncomingDataRowTestMessage(row1_cols).bytes()
          let row2_cols: Array[(String | None)] val = ["row2"]
          let data_row_2 = _IncomingDataRowTestMessage(row2_cols).bytes()

          let notice = _IncomingNoticeResponseTestMessage(
            "NOTICE", "00000", "mid-query notice").bytes()

          let cmd = _IncomingCommandCompleteTestMessage("SELECT 2").bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()

          _tcp_connection.send(row_desc)
          _tcp_connection.send(data_row_1)
          _tcp_connection.send(notice)
          _tcp_connection.send(data_row_2)
          _tcp_connection.send(cmd)
          _tcp_connection.send(ready)
        end
      end
    end

// Integration test

class \nodoc\ iso _TestNoticeOnDropIfExists is UnitTest
  """
  Verifies the pg_notice callback fires when executing
  DROP TABLE IF EXISTS on a nonexistent table through a real PostgreSQL
  server.
  """
  fun name(): String =>
    "integration/Notice/DropIfExists"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)

    let client = _NoticeDropIfExistsClient(h, info)

    h.dispose_when_done(client)
    h.long_test(5_000_000_000)

actor \nodoc\ _NoticeDropIfExistsClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _session: Session
  var _got_notice: Bool = false
  var _got_result: Bool = false

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h

    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS nonexistent_notice_test_xyz"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    _got_result = true

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    _h.complete(false)

  be pg_notice(session: Session, notice: NoticeResponseMessage) =>
    _got_notice = true
    if notice.severity != "NOTICE" then
      _h.fail("Expected severity 'NOTICE' but got '" +
        notice.severity + "'")
      _session.close()
      _h.complete(false)
      return
    end
    if notice.code != "00000" then
      _h.fail("Expected code '00000' but got '" +
        notice.code + "'")
      _session.close()
      _h.complete(false)
      return
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    if _got_result then
      if not _got_notice then
        _h.fail("Got query result but no notice.")
        _session.close()
        _h.complete(false)
        return
      end
      _session.close()
      _h.complete(true)
    end

  be dispose() =>
    _session.close()
