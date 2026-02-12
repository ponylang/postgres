use "buffered"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestNotificationDelivery is UnitTest
  """
  Verifies that pg_notification fires with the correct Notification fields
  when the server sends a NotificationResponse between CommandComplete and
  ReadyForQuery.
  """
  fun name(): String =>
    "Notification/Delivery"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7686"

    let listener = _NotificationTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _NotificationDeliveryClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _NotificationDeliveryClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_result: Bool = false
  var _got_notification: Bool = false

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

  be pg_notification(session: Session, notification: Notification) =>
    _got_notification = true
    if notification.channel != "test_ch" then
      _h.fail("Expected channel 'test_ch' but got '" +
        notification.channel + "'")
      session.close()
      _h.complete(false)
      return
    end
    if notification.payload != "hello" then
      _h.fail("Expected payload 'hello' but got '" +
        notification.payload + "'")
      session.close()
      _h.complete(false)
      return
    end
    if notification.pid != 42 then
      _h.fail("Expected pid 42 but got " + notification.pid.string())
      session.close()
      _h.complete(false)
      return
    end
    // Check will happen in pg_transaction_status after ReadyForQuery
    None

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and notification.
    if _got_result then
      if not _got_notification then
        _h.fail("Got query result but no notification.")
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

class \nodoc\ iso _TestNotificationDuringDataRows is UnitTest
  """
  Verifies that a NotificationResponse arriving between DataRow messages
  still delivers both the complete query result (with all data rows) and the
  notification.
  """
  fun name(): String =>
    "Notification/DuringDataRows"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7687"

    let listener = _NotificationMidQueryTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _NotificationDuringDataRowsClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _NotificationDuringDataRowsClient
  is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  var _got_notification: Bool = false
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

  be pg_notification(session: Session, notification: Notification) =>
    _got_notification = true
    if notification.channel != "ch" then
      _h.fail("Expected channel 'ch' but got '" +
        notification.channel + "'")
      session.close()
      _h.complete(false)
    end

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    // The first pg_transaction_status is from auth ReadyForQuery — skip it.
    // The second one (after query) should have both result and notification.
    if _got_result then
      if not _got_notification then
        _h.fail("Got query result but no notification.")
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

// Shared mock server infrastructure for notification tests

actor \nodoc\ _NotificationTestListener is lori.TCPListenerActor
  """
  Mock server that authenticates, waits for a query, then responds with
  CommandComplete + NotificationResponse + ReadyForQuery.
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

  fun ref _on_accept(fd: U32): _NotificationTestServer =>
    _NotificationTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _NotificationTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  CommandComplete + NotificationResponse + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _authed then
      _authed = true
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    else
      let cmd = _IncomingCommandCompleteTestMessage("SELECT 1").bytes()
      let notif = _IncomingNotificationResponseTestMessage(
        42, "test_ch", "hello").bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(cmd)
      _tcp_connection.send(notif)
      _tcp_connection.send(ready)
    end

actor \nodoc\ _NotificationMidQueryTestListener is lori.TCPListenerActor
  """
  Mock server listener for the mid-query notification test.
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

  fun ref _on_accept(fd: U32): _NotificationMidQueryTestServer =>
    _NotificationMidQueryTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _NotificationMidQueryTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates, then on first query responds with
  RowDescription + DataRow + NotificationResponse + DataRow +
  CommandComplete + ReadyForQuery.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _authed then
      _authed = true
      let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
      let ready = _IncomingReadyForQueryTestMessage('I').bytes()
      _tcp_connection.send(auth_ok)
      _tcp_connection.send(ready)
    else
      try
        // Build RowDescription with one text column
        let columns: Array[(String, String)] val = [("col", "text")]
        let row_desc = _IncomingRowDescriptionTestMessage(columns)?.bytes()

        // Build two DataRow messages
        let row1_cols: Array[(String | None)] val = ["row1"]
        let data_row_1 = _IncomingDataRowTestMessage(row1_cols).bytes()
        let row2_cols: Array[(String | None)] val = ["row2"]
        let data_row_2 = _IncomingDataRowTestMessage(row2_cols).bytes()

        // Build NotificationResponse
        let notif = _IncomingNotificationResponseTestMessage(
          1, "ch", "mid-query").bytes()

        // Build CommandComplete and ReadyForQuery
        let cmd = _IncomingCommandCompleteTestMessage("SELECT 2").bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()

        _tcp_connection.send(row_desc)
        _tcp_connection.send(data_row_1)
        _tcp_connection.send(notif)
        _tcp_connection.send(data_row_2)
        _tcp_connection.send(cmd)
        _tcp_connection.send(ready)
      end
    end
