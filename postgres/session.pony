use "buffered"
use "encode/base64"
use lori = "lori"
use "ssl/crypto"
use "ssl/net"

actor Session is (lori.TCPConnectionActor & lori.ClientLifecycleEventReceiver)
  var state: _SessionState
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _server_connect_info: ServerConnectInfo

  new create(
    server_connect_info': ServerConnectInfo,
    database_connect_info': DatabaseConnectInfo,
    notify': SessionStatusNotify)
  =>
    _server_connect_info = server_connect_info'
    state = _SessionUnopened(notify', database_connect_info',
      server_connect_info'.ssl_mode, server_connect_info'.host)

    _tcp_connection = lori.TCPConnection.client(
      server_connect_info'.auth,
      server_connect_info'.host,
      server_connect_info'.service,
      "",
      this,
      this)

  be execute(query: Query, receiver: ResultReceiver) =>
    """
    Execute a query.
    """
    state.execute(this, query, receiver)

  be prepare(name: String, sql: String, receiver: PrepareReceiver) =>
    """
    Prepare a named server-side statement. The SQL string must contain a single
    statement. On success, `receiver.pg_statement_prepared(session, name)` is called.
    The statement can then be executed with `NamedPreparedQuery(name, params)`.
    """
    state.prepare(this, name, sql, receiver)

  be close_statement(name: String) =>
    """
    Close (destroy) a named prepared statement on the server. Fire-and-forget:
    no callback is issued. It is not an error to close a nonexistent statement.
    """
    state.close_statement(this, name)

  be cancel() =>
    """
    Request cancellation of the currently executing query. Opens a separate
    TCP connection to send a PostgreSQL CancelRequest. Cancellation is
    best-effort — the server may or may not honor it. If cancelled, the
    query's ResultReceiver receives `pg_query_failed` with an ErrorResponse
    (SQLSTATE 57014). Queued queries are not affected.

    Safe to call in any session state. No-op if no query is in flight.
    """
    state.cancel(this)

  be close() =>
    """
    Close the connection. Sends a Terminate message to the server before
    closing the TCP connection. Does not wait for outstanding queries to
    finish.
    """
    state.close(this)

  be _process_again() =>
    state.process_responses(this)

  fun ref _on_connected() =>
    state.on_connected(this)

  fun ref _on_connection_failure() =>
    state.on_failure(this)

  fun ref _on_received(data: Array[U8] iso) =>
    state.on_received(this, consume data)

  // _on_tls_failure already completes cleanup and transitions to
  // _SessionClosed. Lori follows _on_tls_failure() with _on_closed(). Since
  // we don't override _on_closed() (lori's default is a no-op), no additional
  // handling is needed. If _on_closed() is ever added here, it must handle
  // the _SessionClosed state gracefully.
  fun ref _on_tls_ready() =>
    state.on_tls_ready(this)

  fun ref _on_tls_failure() =>
    state.on_tls_failure(this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun server_connect_info(): ServerConnectInfo =>
    _server_connect_info

// Possible session states
class ref _SessionUnopened is _ConnectableState
  let _notify: SessionStatusNotify
  let _database_connect_info: DatabaseConnectInfo
  let _ssl_mode: SSLMode
  let _host: String

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo,
    ssl_mode': SSLMode = SSLDisabled,
    host': String = "")
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'
    _ssl_mode = ssl_mode'
    _host = host'

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver) =>
    r.pg_query_failed(s, q, SessionNeverOpened)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    receiver.pg_prepare_failed(s, name, SessionNeverOpened)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun database_connect_info(): DatabaseConnectInfo =>
    _database_connect_info

  fun ssl_mode(): SSLMode =>
    _ssl_mode

  fun host(): String =>
    _host

  fun notify(): SessionStatusNotify =>
    _notify

class ref _SessionClosed is (_NotConnectableState & _UnconnectedState)
  fun ref execute(s: Session ref, q: Query, r: ResultReceiver) =>
    r.pg_query_failed(s, q, SessionClosed)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    receiver.pg_prepare_failed(s, name, SessionClosed)

  fun ref close_statement(s: Session ref, name: String) =>
    None

class ref _SessionSSLNegotiating
  is (_NotConnectableState & _NotAuthenticableState & _NotAuthenticated)
  """
  Waiting for the server's SSL negotiation response (single byte 'S' or 'N')
  or for the TLS handshake to complete. This state handles raw bytes — the
  server's response to SSLRequest is not a standard PostgreSQL protocol
  message, so _ResponseParser is not used.
  """
  let _notify: SessionStatusNotify
  let _database_connect_info: DatabaseConnectInfo
  let _ssl_ctx: SSLContext val
  let _host: String
  var _handshake_started: Bool = false

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo,
    ssl_ctx': SSLContext val,
    host': String)
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'
    _ssl_ctx = ssl_ctx'
    _host = host'

  fun ref send_ssl_request(s: Session ref) =>
    let msg = _FrontendMessage.ssl_request()
    s._connection().send(msg)

  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    if _handshake_started then
      // Should never happen — lori handles socket I/O during the handshake
      // and doesn't deliver application data until _on_tls_ready.
      _shutdown(s)
      return
    end

    try
      let response = data(0)?
      if response == 'S' then
        match s._connection().start_tls(_ssl_ctx, _host)
        | None =>
          _handshake_started = true
        | let _: lori.StartTLSError =>
          _connection_failed(s)
        end
      elseif response == 'N' then
        _connection_failed(s)
      else
        _shutdown(s)
      end
    else
      _shutdown(s)
    end

  fun ref on_tls_ready(s: Session ref) =>
    // Reset expect from 1 (set during SSLRequest) to 0 (deliver all available
    // bytes). Critical: lori preserves the expect(1) value across start_tls()
    // via _ssl_expect. Without this reset, decrypted data would be delivered
    // 1 byte at a time, breaking _ResponseParser.
    try s._connection().expect(0)? end
    s.state = _SessionConnected(_notify, _database_connect_info)
    _notify.pg_session_connected(s)
    let msg = _FrontendMessage.startup(
      _database_connect_info.user, _database_connect_info.database)
    s._connection().send(msg)

  fun ref on_tls_failure(s: Session ref) =>
    _notify.pg_session_connection_failed(s)
    s.state = _SessionClosed

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver) =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref cancel(s: Session ref) =>
    None

  fun ref close(s: Session ref) =>
    _shutdown(s)

  fun ref shutdown(s: Session ref) =>
    _shutdown(s)

  fun ref _connection_failed(s: Session ref) =>
    s._connection().close()
    _notify.pg_session_connection_failed(s)
    s.state = _SessionClosed

  fun ref _shutdown(s: Session ref) =>
    s._connection().close()
    _notify.pg_session_shutdown(s)
    s.state = _SessionClosed

  fun ref process_responses(s: Session ref) =>
    // No-op: _ResponseMessageParser is not involved during SSL negotiation.
    None

class ref _SessionConnected is _AuthenticableState
  let _notify: SessionStatusNotify
  let _database_connect_info: DatabaseConnectInfo
  let _readbuf: Reader = _readbuf.create()

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo)
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver) =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref on_shutdown(s: Session ref) =>
    // Clearing the readbuf is required for _ResponseMessageParser's
    // synchronous loop to exit — the next parse returns None.
    _readbuf.clear()

  fun user(): String =>
    _database_connect_info.user

  fun password(): String =>
    _database_connect_info.password

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

class ref _SessionSCRAMAuthenticating is (_ConnectedState & _NotAuthenticated)
  """
  Mid-SCRAM-SHA-256 authentication exchange. Has sent the client-first-message
  and is waiting for the server's SASL challenge and final messages.
  """
  let _notify: SessionStatusNotify
  let _readbuf: Reader
  let _client_nonce: String
  let _client_first_bare: String
  let _password: String
  var _expected_server_signature: (Array[U8] val | None) = None

  new ref create(notify': SessionStatusNotify, readbuf': Reader,
    client_nonce': String, client_first_bare': String, password': String)
  =>
    _notify = notify'
    _readbuf = readbuf'
    _client_nonce = client_nonce'
    _client_first_bare = client_first_bare'
    _password = password'

  fun ref on_authentication_ok(s: Session ref) =>
    s.state = _SessionLoggedIn(notify(), readbuf())
    notify().pg_session_authenticated(s)

  fun ref on_authentication_failed(s: Session ref,
    r: AuthenticationFailureReason)
  =>
    notify().pg_session_authentication_failed(s, r)
    shutdown(s)

  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    // Parse server-first-message: r=<combined_nonce>,s=<base64_salt>,i=<iter>
    let server_first: String val = String.from_array(msg.data)
    let parts = server_first.split(",")
    try
      var combined_nonce: String val = ""
      var salt_b64: String val = ""
      var iterations_str: String val = ""

      for part in (consume parts).values() do
        if part.at("r=") then
          let v = part.substring(2)
          combined_nonce = consume v
        elseif part.at("s=") then
          let v = part.substring(2)
          salt_b64 = consume v
        elseif part.at("i=") then
          let v = part.substring(2)
          iterations_str = consume v
        end
      end

      // Validate combined nonce starts with our client nonce
      if not combined_nonce.at(_client_nonce) then
        shutdown(s)
        return
      end

      let salt = Base64.decode[Array[U8] iso](salt_b64)?
      let iterations = iterations_str.u32()?

      (let client_proof, let server_signature) =
        _ScramSha256.compute_proof(_password, consume salt, iterations,
          _client_first_bare, server_first, combined_nonce)?

      let proof_b64_iso = Base64.encode(client_proof)
      let proof_b64: String val = consume proof_b64_iso
      let client_final: String val =
        _ScramSha256.client_final_message(combined_nonce, proof_b64)
      let response: Array[U8] val = client_final.array()
      s._connection().send(_FrontendMessage.sasl_response(response))
      _expected_server_signature = server_signature
    else
      shutdown(s)
    end

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    let server_final: String val = String.from_array(msg.data)

    if server_final.at("e=") then
      on_authentication_failed(s, InvalidPassword)
      return
    end

    if server_final.at("v=") then
      try
        let sig_b64_iso = server_final.substring(2)
        let sig_b64: String val = consume sig_b64_iso
        let received_sig = Base64.decode[Array[U8] iso](sig_b64)?
        match _expected_server_signature
        | let expected: Array[U8] val =>
          if not ConstantTimeCompare(expected, consume received_sig) then
            on_authentication_failed(s, ServerVerificationFailed)
          end
          // If match, wait for AuthenticationOk(0) which PostgreSQL always
          // sends after a successful SASLFinal.
        | None =>
          shutdown(s)
        end
      else
        shutdown(s)
      end
    else
      shutdown(s)
    end

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver) =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref on_shutdown(s: Session ref) =>
    _readbuf.clear()

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

class val _QueuedQuery
  let query: Query
  let receiver: ResultReceiver

  new val create(query': Query, receiver': ResultReceiver) =>
    query = query'
    receiver = receiver'

class val _QueuedPrepare
  let name: String
  let sql: String
  let receiver: PrepareReceiver

  new val create(name': String, sql': String, receiver': PrepareReceiver) =>
    name = name'
    sql = sql'
    receiver = receiver'

class val _QueuedCloseStatement
  let name: String

  new val create(name': String) =>
    name = name'

type _QueueItem is (_QueuedQuery | _QueuedPrepare | _QueuedCloseStatement)

class _SessionLoggedIn is _AuthenticatedState
  """
  An authenticated session ready to execute queries. Query execution is
  managed by a sub-state machine (`_QueryState`) that tracks whether a query
  is in flight, what protocol is active, and owns per-query accumulation data.
  """
  // query_queue, query_state, backend_pid, and backend_secret_key are not
  // underscore-prefixed because other types in this package need access, and
  // Pony private fields are type-private.
  let query_queue: Array[_QueueItem] = query_queue.create()
  var query_state: _QueryState
  var backend_pid: I32 = 0
  var backend_secret_key: I32 = 0
  let _notify: SessionStatusNotify
  let _readbuf: Reader

  new ref create(notify': SessionStatusNotify, readbuf': Reader) =>
    _notify = notify'
    _readbuf = readbuf'
    query_state = _QueryNotReady

  fun ref on_backend_key_data(s: Session ref, msg: _BackendKeyDataMessage) =>
    backend_pid = msg.process_id
    backend_secret_key = msg.secret_key

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    _notify.pg_transaction_status(s, msg.transaction_status())
    query_state.on_ready_for_query(s, this)

  fun ref on_command_complete(s: Session ref, msg: _CommandCompleteMessage) =>
    query_state.on_command_complete(s, this, msg)

  fun ref on_empty_query_response(s: Session ref) =>
    query_state.on_empty_query_response(s, this)

  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage) =>
    query_state.on_error_response(s, this, msg)

  fun ref on_data_row(s: Session ref, msg: _DataRowMessage) =>
    query_state.on_data_row(s, this, msg)

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage) =>
    query_state.on_row_description(s, this, msg)

  fun ref cancel(s: Session ref) =>
    match query_state
    | let _: _QueryReady => None
    | let _: _QueryNotReady => None
    else
      _CancelSender(s.server_connect_info(),
        backend_pid, backend_secret_key)
    end

  fun ref execute(s: Session ref,
    query: Query,
    receiver: ResultReceiver)
  =>
    query_queue.push(_QueuedQuery(query, receiver))
    query_state.try_run_query(s, this)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
  =>
    query_queue.push(_QueuedPrepare(name, sql, receiver))
    query_state.try_run_query(s, this)

  fun ref close_statement(s: Session ref, name: String) =>
    query_queue.push(_QueuedCloseStatement(name))
    query_state.try_run_query(s, this)

  fun ref on_shutdown(s: Session ref) =>
    // Clearing the readbuf is required for _ResponseMessageParser's
    // synchronous loop to exit — the next parse returns None.
    _readbuf.clear()
    for queue_item in query_queue.values() do
      match queue_item
      | let qry: _QueuedQuery =>
        qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
      | let prep: _QueuedPrepare =>
        prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
      | let _: _QueuedCloseStatement => None
      end
    end
    query_queue.clear()

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

// Query sub-state machine
//
// Tracks whether a query is in flight and which protocol is active (simple
// vs extended). The sub-state owns per-query accumulation data, so cleanup
// is structural — data is destroyed when the state transitions out.

interface _QueryState
  """
  Callbacks for query-related protocol messages plus an entry point to
  attempt starting the next queued query.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref)
  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  fun ref on_empty_query_response(s: Session ref, li: _SessionLoggedIn ref)
  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref)

trait _QueryNoQueryInFlight is _QueryState
  """
  Default behavior for states where no query is in flight. Query data
  callbacks and result callbacks trigger shutdown — receiving them without
  an active query indicates a protocol anomaly.
  """
  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage) => li.shutdown(s)
  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage) => li.shutdown(s)
  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage) => li.shutdown(s)
  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref) => li.shutdown(s)
  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage) => li.shutdown(s)
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

class _QueryNotReady is _QueryNoQueryInFlight
  """
  Server has not yet signaled readiness. This is the initial state after
  authentication, before the first ReadyForQuery arrives.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

class _QueryReady is _QueryNoQueryInFlight
  """
  Server has signaled readiness and can accept a query. If the queue is
  non-empty, `try_run_query` immediately transitions to an in-flight state.

  ReadyForQuery while already ready indicates a protocol anomaly — the
  server only sends ReadyForQuery in response to a query cycle or Sync.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      if li.query_queue.size() > 0 then
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          match qry.query
          | let sq: SimpleQuery =>
            li.query_state = _SimpleQueryInFlight.create()
            s._connection().send(_FrontendMessage.query(sq.string))
          | let pq: PreparedQuery =>
            li.query_state = _ExtendedQueryInFlight.create()
            let parse = _FrontendMessage.parse("", pq.string,
              recover val Array[U32] end)
            let bind = _FrontendMessage.bind("", "", pq.params)
            let describe = _FrontendMessage.describe_portal("")
            let execute = _FrontendMessage.execute_msg("", 0)
            let sync = _FrontendMessage.sync()
            let combined = recover val
              let total = parse.size() + bind.size() + describe.size()
                + execute.size() + sync.size()
              let buf = Array[U8](total)
              buf.copy_from(parse, 0, 0, parse.size())
              buf.copy_from(bind, 0, parse.size(), bind.size())
              buf.copy_from(describe, 0,
                parse.size() + bind.size(), describe.size())
              buf.copy_from(execute, 0,
                parse.size() + bind.size() + describe.size(), execute.size())
              buf.copy_from(sync, 0,
                parse.size() + bind.size() + describe.size() + execute.size(),
                sync.size())
              buf
            end
            s._connection().send(consume combined)
          | let nq: NamedPreparedQuery =>
            li.query_state = _ExtendedQueryInFlight.create()
            let bind = _FrontendMessage.bind("", nq.name, nq.params)
            let describe = _FrontendMessage.describe_portal("")
            let execute = _FrontendMessage.execute_msg("", 0)
            let sync = _FrontendMessage.sync()
            let combined = recover val
              let total = bind.size() + describe.size()
                + execute.size() + sync.size()
              let buf = Array[U8](total)
              buf.copy_from(bind, 0, 0, bind.size())
              buf.copy_from(describe, 0, bind.size(), describe.size())
              buf.copy_from(execute, 0,
                bind.size() + describe.size(), execute.size())
              buf.copy_from(sync, 0,
                bind.size() + describe.size() + execute.size(), sync.size())
              buf
            end
            s._connection().send(consume combined)
          end
        | let prep: _QueuedPrepare =>
          li.query_state = _PrepareInFlight.create()
          let parse = _FrontendMessage.parse(prep.name, prep.sql,
            recover val Array[U32] end)
          let describe = _FrontendMessage.describe_statement(prep.name)
          let sync = _FrontendMessage.sync()
          let combined = recover val
            let total = parse.size() + describe.size() + sync.size()
            let buf = Array[U8](total)
            buf.copy_from(parse, 0, 0, parse.size())
            buf.copy_from(describe, 0, parse.size(), describe.size())
            buf.copy_from(sync, 0, parse.size() + describe.size(), sync.size())
            buf
          end
          s._connection().send(consume combined)
        | let cs: _QueuedCloseStatement =>
          li.query_state = _CloseStatementInFlight.create()
          let close = _FrontendMessage.close_statement(cs.name)
          let sync = _FrontendMessage.sync()
          let combined = recover val
            let total = close.size() + sync.size()
            let buf = Array[U8](total)
            buf.copy_from(close, 0, 0, close.size())
            buf.copy_from(sync, 0, close.size(), sync.size())
            buf
          end
          s._connection().send(consume combined)
        end
      end
    else
      _Unreachable()
    end

class _SimpleQueryInFlight is _QueryState
  """
  Simple query protocol in progress. Owns the per-query accumulation data
  which is created fresh for each query and destroyed when the state
  transitions out.
  """
  var _data_rows: Array[Array[(String|None)] val] iso
  var _row_description: (Array[(String, U32)] val | None)

  new create() =>
    _data_rows = recover iso Array[Array[(String|None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(String|None)] val].create()
        end
        let rd = _row_description = None

        match rd
        | let desc: Array[(String, U32)] val =>
          try
            let rows_object = _RowsBuilder(consume rows, desc)?
            qry.receiver.pg_query_result(s,
              ResultSet(qry.query, rows_object, msg.id))
          else
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          end
        | None =>
          if rows.size() > 0 then
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          else
            qry.receiver.pg_query_result(s,
              RowModifying(qry.query, msg.id, msg.value))
          end
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(String|None)] val] end
        let rd = _row_description = None

        if (rows.size() > 0) or (rd isnt None) then
          qry.receiver.pg_query_failed(s, qry.query, DataError)
        else
          qry.receiver.pg_query_result(s, SimpleResult(qry.query))
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(String|None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

class _ExtendedQueryInFlight is _QueryState
  """
  Extended query protocol in progress. Owns the per-query accumulation data
  which is created fresh for each query and destroyed when the state
  transitions out.

  The data accumulation and result delivery logic is identical to
  `_SimpleQueryInFlight`. The duplication exists because Pony traits cannot
  have `iso` fields, so the shared `_data_rows` and `_row_description` state
  cannot be factored into a trait.
  """
  var _data_rows: Array[Array[(String|None)] val] iso
  var _row_description: (Array[(String, U32)] val | None)

  new create() =>
    _data_rows = recover iso Array[Array[(String|None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(String|None)] val].create()
        end
        let rd = _row_description = None

        match rd
        | let desc: Array[(String, U32)] val =>
          try
            let rows_object = _RowsBuilder(consume rows, desc)?
            qry.receiver.pg_query_result(s,
              ResultSet(qry.query, rows_object, msg.id))
          else
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          end
        | None =>
          if rows.size() > 0 then
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          else
            qry.receiver.pg_query_result(s,
              RowModifying(qry.query, msg.id, msg.value))
          end
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(String|None)] val] end
        let rd = _row_description = None

        if (rows.size() > 0) or (rd isnt None) then
          qry.receiver.pg_query_failed(s, qry.query, DataError)
        else
          qry.receiver.pg_query_result(s, SimpleResult(qry.query))
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(String|None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

class _PrepareInFlight is _QueryState
  """
  Prepare (named statement) protocol in progress. Expects ParseComplete,
  ParameterDescription, RowDescription (or NoData), then ReadyForQuery.
  On error, ErrorResponse arrives before ReadyForQuery.
  """
  var _error: Bool = false

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    // Received from Describe(statement) — not cached in this version.
    None

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      match li.query_queue(0)?
      | let prep: _QueuedPrepare =>
        prep.receiver.pg_prepare_failed(s, prep.name, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_statement_prepared(s, prep.name)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    li.shutdown(s)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    li.shutdown(s)

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    li.shutdown(s)

class _CloseStatementInFlight is _QueryState
  """
  Close (named statement) protocol in progress. Expects CloseComplete then
  ReadyForQuery. Fire-and-forget: errors are silently consumed.
  """
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    // Fire-and-forget: ReadyForQuery still arrives to dequeue.
    None

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    li.shutdown(s)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    li.shutdown(s)

  fun ref on_row_description(s: Session ref, li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    li.shutdown(s)

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    li.shutdown(s)

interface _SessionState
  fun on_connected(s: Session ref)
    """
    Called when a connection is established with the server.
    """
  fun on_failure(s: Session ref)
    """
    Called if we fail to establish a connection with the server.
    """
  fun ref on_tls_ready(s: Session ref)
    """
    Called when a TLS handshake initiated by start_tls() completes.
    """
  fun ref on_tls_failure(s: Session ref)
    """
    Called when a TLS handshake initiated by start_tls() fails.
    """
  fun ref on_authentication_ok(s: Session ref)
    """
    Called when we successfully authenticate with the server.
    """
  fun ref on_authentication_failed(
    s: Session ref,
    reason: AuthenticationFailureReason)
    """
    Called if we failed to successfully authenticate with the server.
    """
  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
    """
    Called if the server requests we autheticate using the Postgres MD5
    password scheme.
    """
  fun ref cancel(s: Session ref)
    """
    The client requested query cancellation. Like `close`, this should never
    be an illegal state — it should be silently ignored when not applicable.
    """

  fun ref close(s: Session ref)
    """
    The client received a message to close. Unlike `shutdown`, this should never
    be an illegal state as we can receive messages to take actions from outside
    at any point. If received when "illegal", it should be silently ignored. If
    received when "legal", then `shutdown` should be called.
    """
  fun ref shutdown(s: Session ref)
    """
    Called when we are shutting down the session.
    """
  fun ref on_received(s: Session ref, data: Array[U8] iso)
    """
    Called when we receive data from the server.
    """
  fun ref execute(s: Session ref, query: Query, receiver: ResultReceiver)
    """
    Called when a client requests a query execution.
    """
  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver)
    """
    Called when a client requests a named statement preparation.
    """
  fun ref close_statement(s: Session ref, name: String)
    """
    Called when a client requests closing a named prepared statement.
    """
  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage)
    """
    Called when the server sends a "ready for query" message
    """
  fun ref process_responses(s: Session ref)
    """
    Called to process responses we've received from the server after the data
    has been parsed into messages.
    """
  fun ref on_command_complete(s: Session ref, msg: _CommandCompleteMessage)
    """
    Called when the server has completed running an individual command. If a
    query was a single command, this will be followed by "ready for query". If
    the query contained multiple commands then the results of additional
    commands should be expected. Generally, the arrival of "command complete" is
    when we would want to notify the client of the results or subset of results
    available so far for the active query.

    Queries that resulted in a error will not have "command complete" sent.
    """
  fun ref on_empty_query_response(s: Session ref)
    """
    Called when the server has completed running an individual command that was
    an empty query. This is effectively "command complete" but for the special
    case of "empty query".
    """
  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage)
    """
    Called when the server has encountered an error. Not all errors are called
    using this callback. For example, we intercept authorization errors and
    handle them using a specialized callback. All errors without a specialized
    callback are handled by `on_error_response`.
    """

  fun ref on_data_row(s: Session ref, msg: _DataRowMessage)
    """
    Called when a data row is received from the server.
    """

  fun ref on_backend_key_data(s: Session ref, msg: _BackendKeyDataMessage)
    """
    Called when the server sends BackendKeyData during startup. Contains the
    process ID and secret key needed for query cancellation.
    """

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage)
    """
    Called when a row description is receivedfrom the server.
    """

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
    """
    Called when the server requests SASL authentication, providing a list of
    supported mechanisms.
    """
  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
    """
    Called when the server sends a SASL challenge (server-first-message).
    """
  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
    """
    Called when the server sends a SASL completion (server-final-message).
    """

trait _ConnectableState is _UnconnectedState
  """
  An unopened session that can be connected to a server.
  """
  fun on_connected(s: Session ref) =>
    match ssl_mode()
    | SSLDisabled =>
      s.state = _SessionConnected(notify(), database_connect_info())
      notify().pg_session_connected(s)
      _send_startup_message(s)
    | let req: SSLRequired =>
      // Set expect(1) BEFORE sending SSLRequest so lori delivers exactly
      // one byte per _on_received call. Any MITM-injected bytes stay in
      // lori's internal buffer, causing start_tls() to return
      // StartTLSNotReady (CVE-2021-23222 mitigation).
      try s._connection().expect(1)? end
      let st = _SessionSSLNegotiating(
        notify(), database_connect_info(), req.ctx, host())
      s.state = st
      st.send_ssl_request(s)
    end

  fun on_failure(s: Session ref) =>
    s.state = _SessionClosed
    notify().pg_session_connection_failed(s)

  fun _send_startup_message(s: Session ref) =>
    let dci = database_connect_info()
    let msg = _FrontendMessage.startup(dci.user, dci.database)
    s._connection().send(msg)

  fun database_connect_info(): DatabaseConnectInfo
  fun ssl_mode(): SSLMode
  fun host(): String
  fun notify(): SessionStatusNotify

trait _NotConnectableState
  """
  A session that if it gets messages related to connect to a server, then
  something has gone wrong with the state machine.
  """
  fun on_connected(s: Session ref) =>
    _IllegalState()

  fun on_failure(s: Session ref) =>
    _IllegalState()

trait _ConnectedState is _NotConnectableState
  """
  A connected session. Connected sessions are not connectable as they have
  already been connected.
  """
  fun ref on_tls_ready(s: Session ref) =>
    _IllegalState()

  fun ref on_tls_failure(s: Session ref) =>
    _IllegalState()
  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    readbuf().append(consume data)
    process_responses(s)

  fun ref process_responses(s: Session ref) =>
    _ResponseMessageParser(s, readbuf())

  fun ref cancel(s: Session ref) =>
    None

  fun ref close(s: Session ref) =>
    shutdown(s)

  fun ref shutdown(s: Session ref) =>
    on_shutdown(s)
    s._connection().send(_FrontendMessage.terminate())
    s._connection().close()
    notify().pg_session_shutdown(s)
    s.state = _SessionClosed

  fun ref on_shutdown(s: Session ref) =>
    """
    Called on implementers to allow them to clear state when shutting down.
    """

  fun ref readbuf(): Reader

  fun notify(): SessionStatusNotify

trait _UnconnectedState is (_NotAuthenticableState & _NotAuthenticated)
  """
  A session that isn't connected. Either because it was never opened or because
  it has been closed. Unconnected sessions are not eligible to be authenticated
  and receiving an authentication event while unconnected is an error.
  """
  fun ref on_tls_ready(s: Session ref) =>
    _IllegalState()

  fun ref on_tls_failure(s: Session ref) =>
    _IllegalState()
  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    // It is possible we will continue to receive data after we have closed
    // so this isn't an invalid state. We should silently drop the data. If
    // "not yet opened" and "closed" were different states, rather than a single
    // "unconnected" then we would want to call illegal state if `on_received`
    // was called when the state was "not yet opened".
    None

  fun ref process_responses(s: Session ref) =>
    None

  fun ref cancel(s: Session ref) =>
    None

  fun ref close(s: Session ref) =>
    None

  fun ref shutdown(s: Session ref) =>
    ifdef debug then
      _IllegalState()
    end

trait _AuthenticableState is (_ConnectedState & _NotAuthenticated)
  """
  A session that can be authenticated. All authenticatible sessions are
  connected sessions, but not all connected sessions are autheticable. Once a
  session has been authenticated, it's an error for another authetication event
  to occur.
  """
  fun ref on_authentication_ok(s: Session ref) =>
    s.state = _SessionLoggedIn(notify(), readbuf())
    notify().pg_session_authenticated(s)

  fun ref on_authentication_failed(s: Session ref, r: AuthenticationFailureReason) =>
    notify().pg_session_authentication_failed(s, r)
    shutdown(s)

  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    let md5_password = _MD5Password(user(), password(), msg.salt)
    let reply = _FrontendMessage.password(md5_password)
    s._connection().send(reply)

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
  =>
    // Check if the server supports SCRAM-SHA-256
    var found = false
    for mechanism in msg.mechanisms.values() do
      if mechanism == "SCRAM-SHA-256" then
        found = true
        break
      end
    end
    if not found then
      on_authentication_failed(s, UnsupportedAuthenticationMethod)
      return
    end

    // Generate nonce and build client-first-message
    try
      let nonce_bytes = RandBytes(24)?
      let nonce_iso = Base64.encode(nonce_bytes)
      let nonce: String val = consume nonce_iso
      let client_first_bare: String val =
        _ScramSha256.client_first_message_bare(nonce)
      let client_first: String val =
        _ScramSha256.client_first_message(nonce)
      let response: Array[U8] val = client_first.array()
      s._connection().send(
        _FrontendMessage.sasl_initial_response("SCRAM-SHA-256", response))
      s.state = _SessionSCRAMAuthenticating(
        notify(), readbuf(), nonce, client_first_bare, password())
    else
      shutdown(s)
    end

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    _IllegalState()

  fun user(): String
  fun password(): String
  fun ref readbuf(): Reader
  fun notify(): SessionStatusNotify

trait _NotAuthenticableState
  """
  A session that isn't eligible to be authenticated. Only connected sessions
  that haven't yet been authenticated are eligible to be authenticated.
  """
  fun ref on_authentication_ok(s: Session ref) =>
    _IllegalState()

  fun ref on_authentication_failed(
    s: Session ref,
    r: AuthenticationFailureReason)
  =>
    _IllegalState()

  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    _IllegalState()

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    _IllegalState()

trait _AuthenticatedState is (_ConnectedState & _NotAuthenticableState)
  """
  A connected and authenticated session. Connected sessions are not connectable
  as they have already been connected. Authenticated sessions are not
  authenticable as they have already been authenticated.
  """


trait _NotAuthenticated
  """
  A session that has yet to be authenticated. Before being authenticated, then
  all "query related" commands should not be received.
  """
  fun ref on_backend_key_data(s: Session ref, msg: _BackendKeyDataMessage) =>
    _IllegalState()

  fun ref on_command_complete(s: Session ref, msg: _CommandCompleteMessage) =>
    _IllegalState()

  fun ref on_data_row(s: Session ref, msg: _DataRowMessage) =>
    _IllegalState()

  fun ref on_empty_query_response(s: Session ref) =>
    _IllegalState()

  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage) =>
    _IllegalState()

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    _IllegalState()

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage) =>
    _IllegalState()
