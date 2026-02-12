use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestTransactionStatusOnAuthentication is UnitTest
  """
  Verifies that pg_transaction_status fires with TransactionIdle when the
  initial ReadyForQuery arrives after authentication.
  """
  fun name(): String =>
    "TransactionStatus/OnAuthentication"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7683"

    let listener = _TxnStatusTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _TxnStatusOnAuthClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TxnStatusOnAuthClient is SessionStatusNotify
  let _h: TestHelper
  var _got_status: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    if not _got_status then
      _got_status = true
      if status isnt TransactionIdle then
        _h.fail("Expected TransactionIdle on authentication.")
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

class \nodoc\ iso _TestTransactionStatusDuringTransaction is UnitTest
  """
  Verifies that pg_transaction_status reports TransactionInBlock after BEGIN
  and TransactionIdle after COMMIT.
  """
  fun name(): String =>
    "TransactionStatus/DuringTransaction"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7684"

    let listener = _TxnStatusTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _TxnStatusDuringTxnClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TxnStatusDuringTxnClient is (SessionStatusNotify & ResultReceiver)
  """
  Sends BEGIN then COMMIT and tracks the transaction status sequence:
  idle (auth) -> in-block (after BEGIN) -> idle (after COMMIT).
  """
  let _h: TestHelper
  var _phase: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("BEGIN"), this)

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Initial ReadyForQuery after auth
      if status isnt TransactionIdle then
        _h.fail("Expected TransactionIdle on authentication.")
        session.close()
        _h.complete(false)
      end
    | 2 =>
      // After BEGIN
      if status isnt TransactionInBlock then
        _h.fail("Expected TransactionInBlock after BEGIN.")
        session.close()
        _h.complete(false)
      end
      session.execute(SimpleQuery("COMMIT"), this)
    | 3 =>
      // After COMMIT
      if status isnt TransactionIdle then
        _h.fail("Expected TransactionIdle after COMMIT.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_query_result(session: Session, result: Result) =>
    None

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Unexpected query failure.")
    session.close()
    _h.complete(false)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

class \nodoc\ iso _TestTransactionStatusOnFailedTransaction is UnitTest
  """
  Verifies that pg_transaction_status reports TransactionFailed after an
  error inside a transaction block, then TransactionIdle after ROLLBACK.
  """
  fun name(): String =>
    "TransactionStatus/OnFailedTransaction"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7685"

    let listener = _TxnStatusTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      _TxnStatusFailedClient(h),
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TxnStatusFailedClient is (SessionStatusNotify & ResultReceiver)
  """
  Sends BEGIN, then an invalid query to trigger a failed transaction, then
  ROLLBACK. Tracks the status sequence:
  idle (auth) -> in-block (after BEGIN) -> failed (after error) -> idle (after ROLLBACK).
  """
  let _h: TestHelper
  var _phase: USize = 0

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("BEGIN"), this)

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Initial ReadyForQuery after auth
      if status isnt TransactionIdle then
        _h.fail("Expected TransactionIdle on authentication.")
        session.close()
        _h.complete(false)
      end
    | 2 =>
      // After BEGIN
      if status isnt TransactionInBlock then
        _h.fail("Expected TransactionInBlock after BEGIN.")
        session.close()
        _h.complete(false)
      end
      // Send a query that will fail (non-existent table)
      session.execute(
        SimpleQuery("SELECT * FROM nonexistent_table_txn_test"), this)
    | 3 =>
      // After the error
      if status isnt TransactionFailed then
        _h.fail("Expected TransactionFailed after error.")
        session.close()
        _h.complete(false)
      end
      session.execute(SimpleQuery("ROLLBACK"), this)
    | 4 =>
      // After ROLLBACK
      if status isnt TransactionIdle then
        _h.fail("Expected TransactionIdle after ROLLBACK.")
        session.close()
        _h.complete(false)
        return
      end
      session.close()
      _h.complete(true)
    end

  be pg_query_result(session: Session, result: Result) =>
    None

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    // Expected — the invalid query should fail
    None

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Unable to authenticate.")
    _h.complete(false)

// Shared infrastructure for transaction status tests

actor \nodoc\ _TxnStatusTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _notify: SessionStatusNotify

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    notify: SessionStatusNotify,
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

  fun ref _on_accept(fd: U32): _TxnStatusTestServer =>
    _TxnStatusTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _TxnStatusTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that authenticates and responds to queries with appropriate
  transaction status bytes. Simulates BEGIN/COMMIT/ROLLBACK and error
  scenarios by tracking transaction state.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false
  var _txn_state: U8 = 'I'

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
      // Extract the query string from the SimpleQuery message.
      // Format: 'Q' + Int32(length) + null-terminated query string
      try
        let query = _extract_query(consume data)?
        if query == "BEGIN" then
          _txn_state = 'T'
          let cmd = _IncomingCommandCompleteTestMessage("BEGIN").bytes()
          let ready = _IncomingReadyForQueryTestMessage(_txn_state).bytes()
          _tcp_connection.send(cmd)
          _tcp_connection.send(ready)
        elseif query == "COMMIT" then
          _txn_state = 'I'
          let cmd = _IncomingCommandCompleteTestMessage("COMMIT").bytes()
          let ready = _IncomingReadyForQueryTestMessage(_txn_state).bytes()
          _tcp_connection.send(cmd)
          _tcp_connection.send(ready)
        elseif query == "ROLLBACK" then
          _txn_state = 'I'
          let cmd = _IncomingCommandCompleteTestMessage("ROLLBACK").bytes()
          let ready = _IncomingReadyForQueryTestMessage(_txn_state).bytes()
          _tcp_connection.send(cmd)
          _tcp_connection.send(ready)
        else
          // Unknown query — if in a transaction, simulate an error
          if _txn_state == 'T' then
            _txn_state = 'E'
            let err = _IncomingErrorResponseTestMessage(
              "ERROR", "42P01", "relation does not exist").bytes()
            let ready = _IncomingReadyForQueryTestMessage(_txn_state).bytes()
            _tcp_connection.send(err)
            _tcp_connection.send(ready)
          else
            let cmd = _IncomingCommandCompleteTestMessage("SELECT 0").bytes()
            let ready =
              _IncomingReadyForQueryTestMessage(_txn_state).bytes()
            _tcp_connection.send(cmd)
            _tcp_connection.send(ready)
          end
        end
      end
    end

  fun _extract_query(data: Array[U8] val): String ? =>
    // Skip 'Q' (1 byte) and length (4 bytes), read until null terminator
    if data(0)? != 'Q' then error end
    var i: USize = 5
    let end_idx = data.size() - 1  // last byte is null terminator
    String.from_array(recover val data.slice(i, end_idx) end)
