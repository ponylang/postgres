use "buffered"
use "encode/base64"
use lori = "lori"
use "ssl/crypto"
use "ssl/net"

actor Session is (lori.TCPConnectionActor & lori.ClientLifecycleEventReceiver)
  """
  The main entry point for interacting with a PostgreSQL server. Manages the
  connection lifecycle — connecting, authenticating, executing queries, and
  shutting down — as a state machine.

  Create a session with `ServerConnectInfo` and `DatabaseConnectInfo`.
  Connection and authentication events are delivered to a
  `SessionStatusNotify` receiver.

  Query execution is serialized: only one operation is in flight at a time.
  Additional calls to `execute`, `prepare`, `copy_in`, `copy_out`, `stream`,
  or `pipeline` are queued and dispatched in order. Within a `pipeline` call,
  multiple queries are sent to the server in a single write and processed
  sequentially, reducing round-trip latency.

  An optional connection timeout can be set via `ServerConnectInfo`. If the
  TCP connection is not established within the given duration,
  `pg_session_connection_failed` is called with `ConnectionFailedTimeout`.

  Most operations accept an optional `statement_timeout` parameter. When
  provided, the driver automatically sends a CancelRequest if the operation
  does not complete within the given duration. Construct the timeout with
  `lori.MakeTimerDuration(milliseconds)`.
  """
  var state: _SessionState
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _server_connect_info: ServerConnectInfo

  new create(
    server_connect_info': ServerConnectInfo,
    database_connect_info': DatabaseConnectInfo,
    notify': SessionStatusNotify,
    registry: CodecRegistry = CodecRegistry)
  =>
    _server_connect_info = server_connect_info'
    state = _SessionUnopened(notify', database_connect_info',
      server_connect_info'.ssl_mode, server_connect_info'.host, registry)

    _tcp_connection = lori.TCPConnection.client(
      server_connect_info'.auth,
      server_connect_info'.host,
      server_connect_info'.service,
      "",
      this,
      this
      where connection_timeout = server_connect_info'.connection_timeout)

  be execute(query: Query, receiver: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Execute a query. If `statement_timeout` is provided, the query will be
    cancelled via CancelRequest if it does not complete within the given
    duration.
    """
    state.execute(this, query, receiver, statement_timeout)

  be prepare(name: String, sql: String, receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Prepare a named server-side statement. The SQL string must contain a single
    statement. On success, `receiver.pg_statement_prepared(session, name)` is called.
    The statement can then be executed with `NamedPreparedQuery(name, params)`.
    If `statement_timeout` is provided, the prepare will be cancelled if it does
    not complete within the given duration.
    """
    state.prepare(this, name, sql, receiver, statement_timeout)

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

  be copy_in(sql: String, receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Start a COPY ... FROM STDIN operation. The SQL string should be a COPY
    command with FROM STDIN. On success, the receiver's `pg_copy_ready()` is
    called, and the caller should then send data via `send_copy_data()`,
    finishing with `finish_copy()` or `abort_copy()`. If `statement_timeout`
    is provided, the entire COPY operation (including client data transfer)
    will be cancelled if it does not complete within the given duration.
    """
    state.copy_in(this, sql, receiver, statement_timeout)

  be send_copy_data(data: Array[U8] val) =>
    """
    Send a chunk of data to the server during a COPY IN operation. Data does
    not need to align with row boundaries — the server reassembles the stream.
    No-op if not in COPY IN mode.
    """
    state.send_copy_data(this, data)

  be finish_copy() =>
    """
    Signal successful completion of the COPY data stream. The server will
    validate the data and respond with `pg_copy_complete()` or
    `pg_copy_failed()`. No-op if not in COPY IN mode.
    """
    state.finish_copy(this)

  be abort_copy(reason: String) =>
    """
    Abort the COPY operation with the given error message. The server will
    respond with `pg_copy_failed()`. No-op if not in COPY IN mode.
    """
    state.abort_copy(this, reason)

  be copy_out(sql: String, receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Start a COPY ... TO STDOUT operation. The SQL string should be a COPY
    command with TO STDOUT. Data arrives via the receiver's `pg_copy_data()`
    callback. The operation completes with `pg_copy_complete()` or fails
    with `pg_copy_failed()`. If `statement_timeout` is provided, the COPY
    operation will be cancelled if it does not complete within the given
    duration.
    """
    state.copy_out(this, sql, receiver, statement_timeout)

  be stream(query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Start a streaming query that delivers rows in windowed batches via
    `StreamingResultReceiver`. Each batch contains up to `window_size` rows.
    Call `fetch_more()` from `pg_stream_batch` to pull the next batch, or
    `close_stream()` to end early.

    Only `PreparedQuery` and `NamedPreparedQuery` are supported — streaming
    uses the extended query protocol's `Execute(max_rows)` + `PortalSuspended`
    mechanism which requires a prepared statement.

    If `statement_timeout` is provided, the entire streaming operation (from
    initial Execute to final ReadyForQuery) will be cancelled if it does not
    complete within the given duration.
    """
    state.stream(this, query, window_size, receiver, statement_timeout)

  be fetch_more() =>
    """
    Request the next batch of rows during a streaming query. The next
    `pg_stream_batch` callback delivers the rows. Safe to call at any
    time — no-op if no streaming query is active, if the stream has
    already completed naturally, or if the stream has already failed.
    """
    state.fetch_more(this)

  be close_stream() =>
    """
    End a streaming query early. The `pg_stream_complete` callback fires
    when the server acknowledges the close. Safe to call at any time —
    no-op if no streaming query is active, if the stream has already
    completed naturally, or if the stream has already failed.
    """
    state.close_stream(this)

  be pipeline(queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    """
    Execute multiple queries in a single pipeline. All queries are sent to the
    server in one TCP write and processed in order, reducing round-trip latency
    from N round trips to 1. Each query has its own Sync boundary for error
    isolation — if one query fails, subsequent queries continue executing.

    Results are delivered via `PipelineReceiver` with an index corresponding to
    each query's position in the array. `pg_pipeline_complete` always fires
    last. Only `PreparedQuery` and `NamedPreparedQuery` are supported —
    pipelining uses the extended query protocol.

    If `statement_timeout` is provided, the entire pipeline will be cancelled
    if it does not complete within the given duration.
    """
    state.pipeline(this, queries, receiver, statement_timeout)

  be close() =>
    """
    Close the connection. Sends a Terminate message to the server before
    closing the TCP connection. Does not wait for outstanding queries to
    finish.
    """
    state.close(this)

  be _process_again() =>
    state.process_responses(this)

  fun ref _on_timer(token: lori.TimerToken) =>
    state.on_timer(this, token)

  fun ref _on_connected() =>
    state.on_connected(this)

  fun ref _on_connection_failure(reason: lori.ConnectionFailureReason) =>
    let r: ConnectionFailureReason = match \exhaustive\ reason
    | let _: lori.ConnectionFailedDNS => ConnectionFailedDNS
    | let _: lori.ConnectionFailedTCP => ConnectionFailedTCP
    | let _: lori.ConnectionFailedSSL => TLSHandshakeFailed
    | let _: lori.ConnectionFailedTimeout => ConnectionFailedTimeout
    | let _: lori.ConnectionFailedTimerError => ConnectionFailedTimerError
    end
    state.on_connection_failed(this, r)

  fun ref _on_received(data: Array[U8] iso) =>
    state.on_received(this, consume data)

  // Routed through the state machine. Each state handles peer close
  // through its own `on_closed` — pre-ready states deliver
  // `pg_session_connection_failed(ConnectionClosedByServer)`;
  // `_SessionLoggedIn` notifies any in-flight query; `_SessionClosed`
  // is a no-op so lori's follow-up after user-initiated close or TLS
  // failure does not double-notify.
  fun ref _on_closed() =>
    state.on_closed(this)

  fun ref _on_tls_ready() =>
    state.on_tls_ready(this)

  fun ref _on_tls_failure(reason: lori.TLSFailureReason) =>
    let r: ConnectionFailureReason = match \exhaustive\ reason
    | let _: lori.TLSAuthFailed => TLSAuthFailed
    | let _: lori.TLSGeneralError => TLSHandshakeFailed
    end
    state.on_connection_failed(this, r)

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
  let _codec_registry: CodecRegistry

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo,
    ssl_mode': SSLMode = SSLDisabled,
    host': String = "",
    codec_registry': CodecRegistry = CodecRegistry)
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'
    _ssl_mode = ssl_mode'
    _host = host'
    _codec_registry = codec_registry'

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    r.pg_query_failed(s, q, SessionNeverOpened)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_prepare_failed(s, name, SessionNeverOpened)

  fun ref copy_in(s: Session ref, sql: String,
    receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNeverOpened)

  fun ref copy_out(s: Session ref, sql: String,
    receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNeverOpened)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_stream_failed(s, query, SessionNeverOpened)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    for (i, q) in queries.pairs() do
      receiver.pg_pipeline_failed(s, i, q, SessionNeverOpened)
    end
    receiver.pg_pipeline_complete(s)

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

  fun codec_registry(): CodecRegistry =>
    _codec_registry

  fun ref on_protocol_violation(s: Session ref) =>
    _IllegalState()

  fun ref on_closed(s: Session ref) =>
    // No TCP connection has ever existed in this state — lori cannot fire
    // `_on_closed` before `_on_connected` or `_on_connection_failure`.
    _IllegalState()

class ref _SessionClosed is (_NotConnectableState & _UnconnectedState)
  fun ref execute(s: Session ref, q: Query, r: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    r.pg_query_failed(s, q, SessionClosed)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_prepare_failed(s, name, SessionClosed)

  fun ref copy_in(s: Session ref, sql: String,
    receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionClosed)

  fun ref copy_out(s: Session ref, sql: String,
    receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionClosed)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_stream_failed(s, query, SessionClosed)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    for (i, q) in queries.pairs() do
      receiver.pg_pipeline_failed(s, i, q, SessionClosed)
    end
    receiver.pg_pipeline_complete(s)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    _IllegalState()

  fun ref on_protocol_violation(s: Session ref) =>
    _IllegalState()

  fun ref on_closed(s: Session ref) =>
    // Reachable after the session has already shut down and transitioned
    // here: either user-initiated close (lori fires `_on_closed` after
    // the resulting `hard_close()`), or a TLS-handshake failure where
    // lori fires `_on_tls_failure` followed by `_on_closed`. Firing
    // callbacks again would double-notify the application.
    None

class ref _SessionSSLNegotiating
  is (_NotConnectableState & _NotAuthenticableState & _NotAuthenticated)
  """
  Waiting for the server's SSL negotiation response (single byte 'S' or 'N')
  or for the TLS handshake to complete. This state handles raw bytes — the
  server's response to SSLRequest is not a standard PostgreSQL protocol
  message, so _ResponseParser is not used.

  `_fallback_on_refusal` controls behavior when the server responds 'N': if
  true (`SSLPreferred`), the session falls back to plaintext; if false
  (`SSLRequired`), the session fires `pg_session_connection_failed`. TLS
  handshake failures always fire `pg_session_connection_failed` regardless
  of this flag. If the server closes the TCP connection during negotiation,
  `pg_session_connection_failed(ConnectionClosedByServer)` fires.
  """
  let _notify: SessionStatusNotify
  let _database_connect_info: DatabaseConnectInfo
  let _ssl_ctx: SSLContext val
  let _host: String
  let _fallback_on_refusal: Bool
  let _codec_registry: CodecRegistry
  var _handshake_started: Bool = false

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo,
    ssl_ctx': SSLContext val,
    host': String,
    fallback_on_refusal': Bool,
    codec_registry': CodecRegistry = CodecRegistry)
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'
    _ssl_ctx = ssl_ctx'
    _host = host'
    _fallback_on_refusal = fallback_on_refusal'
    _codec_registry = codec_registry'

  fun ref send_ssl_request(s: Session ref) =>
    let msg = _FrontendMessage.ssl_request()
    s._connection().send(msg)

  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    if _handshake_started then
      // Invariant: lori handles socket I/O during the TLS handshake and does
      // not deliver application data until on_tls_ready fires. Reaching this
      // branch means lori's contract has been violated — a crash is the
      // correct response.
      _IllegalState()
    end

    try
      let response = data(0)?
      if response == 'S' then
        match \exhaustive\ s._connection().start_tls(_ssl_ctx, _host)
        | None =>
          _handshake_started = true
        | let _: lori.StartTLSError =>
          _connection_failed(s, TLSHandshakeFailed)
        end
      elseif response == 'N' then
        if _fallback_on_refusal then
          _proceed_to_connected(s)
        else
          _connection_failed(s, SSLServerRefused)
        end
      else
        _connection_failed(s, ProtocolViolation)
      end
    else
      _Unreachable()
    end

  fun ref on_tls_ready(s: Session ref) =>
    _proceed_to_connected(s)

  fun ref _proceed_to_connected(s: Session ref) =>
    // Reset buffer_until from 1 (set during SSLRequest) to streaming (deliver
    // all available bytes). Critical: lori preserves the buffer_until value
    // across start_tls(). Without this reset, decrypted data would be delivered
    // 1 byte at a time, breaking _ResponseParser.
    s._connection().buffer_until(lori.Streaming)
    s.state = _SessionConnected(_notify, _database_connect_info,
      _codec_registry)
    _notify.pg_session_connected(s)
    let msg = _FrontendMessage.startup(
      _database_connect_info.user, _database_connect_info.database)
    s._connection().send(msg)

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    """
    Entry point for failures reported by lori — the TLS handshake failure
    path (Session._on_tls_failure) and the peer-close path
    (Session._on_closed, routed via `on_closed` below). Lori has already
    closed the TCP connection on its side by the time this fires, so we
    do not call close() ourselves.

    For internally-initiated failures where the TCP connection is still
    live (e.g., a bad SSL-negotiation response byte, StartTLSError), use
    `_connection_failed` instead.
    """
    _notify.pg_session_connection_failed(s, reason)
    _notify.pg_session_shutdown(s)
    s.state = _SessionClosed

  fun ref on_closed(s: Session ref) =>
    on_connection_failed(s, ConnectionClosedByServer)

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref copy_in(s: Session ref, sql: String,
    receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref copy_out(s: Session ref, sql: String,
    receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_stream_failed(s, query, SessionNotAuthenticated)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    for (i, q) in queries.pairs() do
      receiver.pg_pipeline_failed(s, i, q, SessionNotAuthenticated)
    end
    receiver.pg_pipeline_complete(s)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref send_copy_data(s: Session ref, data: Array[U8] val) =>
    None

  fun ref finish_copy(s: Session ref) =>
    None

  fun ref abort_copy(s: Session ref, reason: String) =>
    None

  fun ref fetch_more(s: Session ref) =>
    None

  fun ref close_stream(s: Session ref) =>
    None

  fun ref on_notice(s: Session ref, msg: NoticeResponseMessage) =>
    _IllegalState()

  fun ref on_parameter_status(s: Session ref,
    msg: _ParameterStatusMessage)
  =>
    _IllegalState()

  fun ref on_portal_suspended(s: Session ref) =>
    _IllegalState()

  fun ref cancel(s: Session ref) =>
    None

  fun ref on_timer(s: Session ref, token: lori.TimerToken) =>
    None

  fun ref close(s: Session ref) =>
    _shutdown(s)

  fun ref shutdown(s: Session ref) =>
    _shutdown(s)

  fun ref on_protocol_violation(s: Session ref) =>
    // Parser does not run during SSL negotiation; protocol violations in
    // this state are routed through `_connection_failed` directly from
    // `on_received`.
    _IllegalState()

  fun ref _connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    """
    Entry point for internally-initiated failures during SSL negotiation
    (e.g., a non-'S'/'N' response byte, StartTLSError). The TCP connection
    is still live, so this helper closes it before firing the failure and
    shutdown callbacks.

    For failures reported by lori (where lori has already closed the TCP
    connection), use `on_connection_failed` instead.
    """
    s._connection().close()
    _notify.pg_session_connection_failed(s, reason)
    _notify.pg_session_shutdown(s)
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
  let _codec_registry: CodecRegistry
  let _readbuf: Reader = _readbuf.create()

  new ref create(notify': SessionStatusNotify,
    database_connect_info': DatabaseConnectInfo,
    codec_registry': CodecRegistry = CodecRegistry)
  =>
    _notify = notify'
    _database_connect_info = database_connect_info'
    _codec_registry = codec_registry'

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref copy_in(s: Session ref, sql: String,
    receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref copy_out(s: Session ref, sql: String,
    receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_stream_failed(s, query, SessionNotAuthenticated)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    for (i, q) in queries.pairs() do
      receiver.pg_pipeline_failed(s, i, q, SessionNotAuthenticated)
    end
    receiver.pg_pipeline_complete(s)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref on_shutdown(s: Session ref) =>
    // Clearing the readbuf is required for _ResponseMessageParser's
    // synchronous loop to exit — the next parse returns None.
    _readbuf.clear()

  fun ref on_closed(s: Session ref) =>
    // Direct implementation: the `_AuthenticableState.on_connection_failed`
    // trait default chains into `_ConnectedState.shutdown`, which sends
    // `Terminate` and calls `close()` on an already-closed connection.
    on_shutdown(s)
    notify().pg_session_connection_failed(s, ConnectionClosedByServer)
    notify().pg_session_shutdown(s)
    s.state = _SessionClosed

  fun user(): String =>
    _database_connect_info.user

  fun password(): String =>
    _database_connect_info.password

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

  fun codec_registry(): CodecRegistry =>
    _codec_registry

class ref _SessionSCRAMAuthenticating is (_ConnectedState & _NotAuthenticated)
  """
  Mid-SCRAM-SHA-256 authentication exchange. Has sent the client-first-message
  and is waiting for the server's SASL challenge and final messages.

  Enforces SCRAM's mutual-authentication property: transitioning to
  `_SessionLoggedIn` requires that the server's `v=<verifier>` SASLFinal was
  received and compared equal to the locally computed signature via
  `ConstantTimeCompare`. The `_server_verified` flag records this; any
  protocol violation (skipped, duplicated, or malformed SASL messages;
  mismatched signature; mismatched nonce) is reported to the application as
  `pg_session_connection_failed(ServerVerificationFailed)`.
  """
  let _notify: SessionStatusNotify
  let _readbuf: Reader
  let _client_nonce: String
  let _client_first_bare: String
  let _password: String
  let _codec_registry: CodecRegistry
  var _expected_server_signature: (Array[U8] val | None) = None
  var _server_verified: Bool = false

  new ref create(notify': SessionStatusNotify, readbuf': Reader,
    client_nonce': String, client_first_bare': String, password': String,
    codec_registry': CodecRegistry = CodecRegistry)
  =>
    _notify = notify'
    _readbuf = readbuf'
    _client_nonce = client_nonce'
    _client_first_bare = client_first_bare'
    _password = password'
    _codec_registry = codec_registry'

  fun ref on_authentication_ok(s: Session ref) =>
    if not _server_verified then
      on_connection_failed(s, ServerVerificationFailed)
      return
    end
    s.state = _SessionLoggedIn(notify(), readbuf(), _codec_registry)
    notify().pg_session_authenticated(s)

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    notify().pg_session_connection_failed(s, reason)
    shutdown(s)

  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage) =>
    on_connection_failed(s, _ConnectionFailureReasonFromError(msg))

  fun ref on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    on_protocol_violation(s)

  fun ref on_authentication_cleartext_password(s: Session ref) =>
    on_protocol_violation(s)

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
  =>
    on_protocol_violation(s)

  fun ref on_protocol_violation(s: Session ref) =>
    on_connection_failed(s, ProtocolViolation)

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    // A second SASLContinue would overwrite the verifier, resetting
    // verification state. Reject as a protocol violation.
    if _expected_server_signature isnt None then
      on_connection_failed(s, ServerVerificationFailed)
      return
    end
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
        on_connection_failed(s, ServerVerificationFailed)
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
      on_connection_failed(s, ServerVerificationFailed)
    end

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    let server_final: String val = String.from_array(msg.data)

    if server_final.at("v=") then
      try
        let sig_b64_iso = server_final.substring(2)
        let sig_b64: String val = consume sig_b64_iso
        let received_sig = Base64.decode[Array[U8] iso](sig_b64)?
        match \exhaustive\ _expected_server_signature
        | let expected: Array[U8] val =>
          if not ConstantTimeCompare(expected, consume received_sig) then
            on_connection_failed(s, ServerVerificationFailed)
            return
          end
          _server_verified = true
          // On match, wait for AuthenticationOk(0) which PostgreSQL always
          // sends after a successful SASLFinal.
        | None =>
          on_connection_failed(s, ServerVerificationFailed)
          return
        end
      else
        on_connection_failed(s, ServerVerificationFailed)
      end
    else
      on_connection_failed(s, ServerVerificationFailed)
    end

  fun ref execute(s: Session ref, q: Query, r: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    r.pg_query_failed(s, q, SessionNotAuthenticated)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_prepare_failed(s, name, SessionNotAuthenticated)

  fun ref copy_in(s: Session ref, sql: String,
    receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref copy_out(s: Session ref, sql: String,
    receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_copy_failed(s, SessionNotAuthenticated)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    receiver.pg_stream_failed(s, query, SessionNotAuthenticated)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    for (i, q) in queries.pairs() do
      receiver.pg_pipeline_failed(s, i, q, SessionNotAuthenticated)
    end
    receiver.pg_pipeline_complete(s)

  fun ref close_statement(s: Session ref, name: String) =>
    None

  fun ref on_shutdown(s: Session ref) =>
    _readbuf.clear()

  fun ref on_closed(s: Session ref) =>
    // Direct implementation: `on_connection_failed` chains into
    // `_ConnectedState.shutdown`, which sends `Terminate` and calls
    // `close()` on an already-closed connection.
    on_shutdown(s)
    notify().pg_session_connection_failed(s, ConnectionClosedByServer)
    notify().pg_session_shutdown(s)
    s.state = _SessionClosed

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

class val _QueuedQuery
  let query: Query
  let receiver: ResultReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(query': Query, receiver': ResultReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    query = query'
    receiver = receiver'
    statement_timeout = statement_timeout'

class val _QueuedPrepare
  let name: String
  let sql: String
  let receiver: PrepareReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(name': String, sql': String, receiver': PrepareReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    name = name'
    sql = sql'
    receiver = receiver'
    statement_timeout = statement_timeout'

class val _QueuedCloseStatement
  let name: String

  new val create(name': String) =>
    name = name'

class val _QueuedCopyIn
  let sql: String
  let receiver: CopyInReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(sql': String, receiver': CopyInReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    sql = sql'
    receiver = receiver'
    statement_timeout = statement_timeout'

class val _QueuedCopyOut
  let sql: String
  let receiver: CopyOutReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(sql': String, receiver': CopyOutReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    sql = sql'
    receiver = receiver'
    statement_timeout = statement_timeout'

class val _QueuedStreamingQuery
  """
  A queued streaming query operation waiting to be dispatched.
  """
  let query: (PreparedQuery | NamedPreparedQuery)
  let window_size: U32
  let receiver: StreamingResultReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(query': (PreparedQuery | NamedPreparedQuery),
    window_size': U32, receiver': StreamingResultReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    query = query'
    window_size = window_size'
    receiver = receiver'
    statement_timeout = statement_timeout'

class val _QueuedPipeline
  """
  A queued pipeline operation waiting to be dispatched.
  """
  let queries: Array[(PreparedQuery | NamedPreparedQuery)] val
  let receiver: PipelineReceiver
  let statement_timeout: (lori.TimerDuration | None)

  new val create(queries': Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver': PipelineReceiver,
    statement_timeout': (lori.TimerDuration | None) = None)
  =>
    queries = queries'
    receiver = receiver'
    statement_timeout = statement_timeout'

type _QueueItem is
  ( _QueuedQuery
  | _QueuedPrepare
  | _QueuedCloseStatement
  | _QueuedCopyIn
  | _QueuedCopyOut
  | _QueuedStreamingQuery
  | _QueuedPipeline )

class _SessionLoggedIn is _AuthenticatedState
  """
  An authenticated session ready to execute queries. Query execution is
  managed by a sub-state machine (`_QueryState`) that tracks whether a query
  is in flight, what protocol is active, and owns per-query accumulation data.
  """
  // query_queue, query_state, backend_pid, backend_secret_key,
  // codec_registry, and statement_timer are not underscore-prefixed because
  // other types in this package need access, and Pony private fields are
  // type-private.
  let query_queue: Array[_QueueItem] = query_queue.create()
  var query_state: _QueryState
  var backend_pid: I32 = 0
  var backend_secret_key: I32 = 0
  let codec_registry: CodecRegistry
  var statement_timer: (lori.TimerToken | None) = None
  let _notify: SessionStatusNotify
  let _readbuf: Reader

  new ref create(notify': SessionStatusNotify, readbuf': Reader,
    codec_registry': CodecRegistry = CodecRegistry)
  =>
    _notify = notify'
    _readbuf = readbuf'
    codec_registry = codec_registry'
    query_state = _QueryNotReady

  fun ref on_notification(s: Session ref, msg: _NotificationResponseMessage) =>
    _notify.pg_notification(s,
      Notification(msg.channel, msg.payload, msg.process_id))

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

  fun ref on_portal_suspended(s: Session ref) =>
    query_state.on_portal_suspended(s, this)

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    _IllegalState()

  fun ref on_protocol_violation(s: Session ref) =>
    // Let the in-flight query (if any) deliver a ProtocolViolation to its
    // receiver. drain_in_flight (called from shutdown's on_shutdown chain)
    // will skip the already-notified item because `_error = true`.
    query_state.on_protocol_violation(s, this)
    shutdown(s)

  fun ref on_closed(s: Session ref) =>
    // Server closed the TCP connection. The `shutdown` helper in
    // `_ConnectedState` would send `Terminate` and call `close()` on the
    // already-gone connection, so we open-code the cleanup instead:
    // notify the in-flight query, drain the queue via `on_shutdown`, then
    // fire `pg_session_shutdown` and transition.
    query_state.on_closed(s, this)
    on_shutdown(s)
    _notify.pg_session_shutdown(s)
    s.state = _SessionClosed

  fun ref on_timer(s: Session ref, token: lori.TimerToken) =>
    match statement_timer
    | let t: lori.TimerToken if t == token =>
      statement_timer = None
      _CancelSender(s.server_connect_info(), backend_pid, backend_secret_key)
    end

  fun ref cancel_statement_timer(s: Session ref) =>
    match statement_timer
    | let t: lori.TimerToken =>
      s._connection().cancel_timer(t)
      statement_timer = None
    end

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
    receiver: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedQuery(query, receiver, statement_timeout))
    query_state.try_run_query(s, this)

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedPrepare(name, sql, receiver, statement_timeout))
    query_state.try_run_query(s, this)

  fun ref close_statement(s: Session ref, name: String) =>
    query_queue.push(_QueuedCloseStatement(name))
    query_state.try_run_query(s, this)

  fun ref copy_in(s: Session ref, sql: String, receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedCopyIn(sql, receiver, statement_timeout))
    query_state.try_run_query(s, this)

  fun ref copy_out(s: Session ref, sql: String, receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedCopyOut(sql, receiver, statement_timeout))
    query_state.try_run_query(s, this)

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedStreamingQuery(query, window_size, receiver,
      statement_timeout))
    query_state.try_run_query(s, this)

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
  =>
    query_queue.push(_QueuedPipeline(queries, receiver, statement_timeout))
    query_state.try_run_query(s, this)

  fun ref fetch_more(s: Session ref) =>
    match query_state
    | let sq: _StreamingQueryInFlight => sq.fetch_more(s, this)
    end

  fun ref close_stream(s: Session ref) =>
    match query_state
    | let sq: _StreamingQueryInFlight => sq.close_stream(s)
    end

  fun ref send_copy_data(s: Session ref, data: Array[U8] val) =>
    match query_state
    | let c: _CopyInInFlight => c.send_copy_data(s, this, data)
    end

  fun ref finish_copy(s: Session ref) =>
    match query_state
    | let c: _CopyInInFlight => c.finish_copy(s)
    end

  fun ref abort_copy(s: Session ref, reason: String) =>
    match query_state
    | let c: _CopyInInFlight => c.abort_copy(s, reason)
    end

  fun ref on_copy_in_response(s: Session ref, msg: _CopyInResponseMessage) =>
    query_state.on_copy_in_response(s, this, msg)

  fun ref on_copy_out_response(s: Session ref,
    msg: _CopyOutResponseMessage)
  =>
    query_state.on_copy_out_response(s, this, msg)

  fun ref on_copy_data(s: Session ref, msg: _CopyDataMessage) =>
    query_state.on_copy_data(s, this, msg)

  fun ref on_copy_done(s: Session ref) =>
    query_state.on_copy_done(s, this)

  fun ref on_shutdown(s: Session ref) =>
    // Cancel any active statement timeout timer. Lori's hard_close() cancels
    // the ASIO timer, but we clear our field for consistency.
    cancel_statement_timer(s)
    // Clearing the readbuf is required for _ResponseMessageParser's
    // synchronous loop to exit — the next parse returns None.
    _readbuf.clear()
    // The in-flight item (if any) may have already notified its receiver
    // via on_error_response. Let the query state handle it to avoid
    // double-notification, then drain the remaining queued items.
    query_state.drain_in_flight(s, this)
    for queue_item in query_queue.values() do
      match \exhaustive\ queue_item
      | let qry: _QueuedQuery =>
        qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
      | let prep: _QueuedPrepare =>
        prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
      | let _: _QueuedCloseStatement => None
      | let ci: _QueuedCopyIn =>
        ci.receiver.pg_copy_failed(s, SessionClosed)
      | let co: _QueuedCopyOut =>
        co.receiver.pg_copy_failed(s, SessionClosed)
      | let sq: _QueuedStreamingQuery =>
        sq.receiver.pg_stream_failed(s, sq.query, SessionClosed)
      | let pl: _QueuedPipeline =>
        for (i, q) in pl.queries.pairs() do
          pl.receiver.pg_pipeline_failed(s, i, q, SessionClosed)
        end
        pl.receiver.pg_pipeline_complete(s)
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
  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref)
  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called when a portal is suspended during streaming (more rows available).
    """
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref)
  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref)
  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called from `_SessionLoggedIn.on_protocol_violation`. In-flight states
    deliver a `ProtocolViolation` failure to their receiver and set their
    internal `_error` flag so the subsequent `drain_in_flight` skips the
    already-notified item. States with no query in flight are a no-op.
    """
  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called from `_SessionLoggedIn.on_closed` when the server closes the TCP
    connection. In-flight states deliver a `SessionClosed` failure to their
    receiver and set their internal `_error` flag so the subsequent
    `drain_in_flight` skips the already-notified item. States with no query
    in flight are a no-op.
    """

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
  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage) => li.shutdown(s)
  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage) => li.shutdown(s)
  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage) => li.shutdown(s)
  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)
  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None
  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) => None
  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    None
  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    None

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
        match \exhaustive\ li.query_queue(0)?
        | let qry: _QueuedQuery =>
          match \exhaustive\ qry.query
          | let sq: SimpleQuery =>
            li.query_state = _SimpleQueryInFlight.create()
            s._connection().send(_FrontendMessage.query(sq.string))
          | let pq: PreparedQuery =>
            // Build messages before transitioning state so an encode
            // error in bind() leaves the state machine in _QueryReady.
            let combined = try
              let parse = _FrontendMessage.parse("", pq.string,
                _ParamEncoder.oids_for(pq.params, li.codec_registry))
              let bind = _FrontendMessage.bind("", "", pq.params,
                li.codec_registry)?
              let describe = _FrontendMessage.describe_portal("")
              let execute = _FrontendMessage.execute_msg("", 0)
              let sync = _FrontendMessage.sync()
              recover val
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
                  parse.size() + bind.size() + describe.size()
                    + execute.size(),
                  sync.size())
                buf
              end
            else
              qry.receiver.pg_query_failed(s, qry.query, DataError)
              try li.query_queue.shift()? else _Unreachable() end
              try_run_query(s, li)
              return
            end
            li.query_state = _ExtendedQueryInFlight.create()
            s._connection().send(combined)
          | let nq: NamedPreparedQuery =>
            let combined = try
              let bind = _FrontendMessage.bind("", nq.name, nq.params,
                li.codec_registry)?
              let describe = _FrontendMessage.describe_portal("")
              let execute = _FrontendMessage.execute_msg("", 0)
              let sync = _FrontendMessage.sync()
              recover val
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
            else
              qry.receiver.pg_query_failed(s, qry.query, DataError)
              try li.query_queue.shift()? else _Unreachable() end
              try_run_query(s, li)
              return
            end
            li.query_state = _ExtendedQueryInFlight.create()
            s._connection().send(combined)
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
        | let ci: _QueuedCopyIn =>
          li.query_state = _CopyInInFlight
          s._connection().send(_FrontendMessage.query(ci.sql))
        | let co: _QueuedCopyOut =>
          li.query_state = _CopyOutInFlight
          s._connection().send(_FrontendMessage.query(co.sql))
        | let sq: _QueuedStreamingQuery =>
          match \exhaustive\ sq.query
          | let pq: PreparedQuery =>
            let combined = try
              let parse = _FrontendMessage.parse("", pq.string,
                _ParamEncoder.oids_for(pq.params, li.codec_registry))
              let bind = _FrontendMessage.bind("", "", pq.params,
                li.codec_registry)?
              let describe = _FrontendMessage.describe_portal("")
              let execute = _FrontendMessage.execute_msg("", sq.window_size)
              let flush_msg = _FrontendMessage.flush()
              recover val
                let total = parse.size() + bind.size() + describe.size()
                  + execute.size() + flush_msg.size()
                let buf = Array[U8](total)
                buf.copy_from(parse, 0, 0, parse.size())
                buf.copy_from(bind, 0, parse.size(), bind.size())
                buf.copy_from(describe, 0,
                  parse.size() + bind.size(), describe.size())
                buf.copy_from(execute, 0,
                  parse.size() + bind.size() + describe.size(), execute.size())
                buf.copy_from(flush_msg, 0,
                  parse.size() + bind.size() + describe.size()
                    + execute.size(),
                  flush_msg.size())
                buf
              end
            else
              sq.receiver.pg_stream_failed(s, sq.query, DataError)
              try li.query_queue.shift()? else _Unreachable() end
              try_run_query(s, li)
              return
            end
            li.query_state = _StreamingQueryInFlight.create()
            s._connection().send(combined)
          | let nq: NamedPreparedQuery =>
            let combined = try
              let bind = _FrontendMessage.bind("", nq.name, nq.params,
                li.codec_registry)?
              let describe = _FrontendMessage.describe_portal("")
              let execute = _FrontendMessage.execute_msg("", sq.window_size)
              let flush_msg = _FrontendMessage.flush()
              recover val
                let total = bind.size() + describe.size()
                  + execute.size() + flush_msg.size()
                let buf = Array[U8](total)
                buf.copy_from(bind, 0, 0, bind.size())
                buf.copy_from(describe, 0, bind.size(), describe.size())
                buf.copy_from(execute, 0,
                  bind.size() + describe.size(), execute.size())
                buf.copy_from(flush_msg, 0,
                  bind.size() + describe.size() + execute.size(),
                  flush_msg.size())
                buf
              end
            else
              sq.receiver.pg_stream_failed(s, sq.query, DataError)
              try li.query_queue.shift()? else _Unreachable() end
              try_run_query(s, li)
              return
            end
            li.query_state = _StreamingQueryInFlight.create()
            s._connection().send(combined)
          end
        | let pl: _QueuedPipeline =>
          if pl.queries.size() == 0 then
            pl.receiver.pg_pipeline_complete(s)
            try
              li.query_queue.shift()?
            else
              _Unreachable()
            end
            try_run_query(s, li)
            return
          end
          let parts = recover iso Array[Array[U8] val] end
          for (qi, query) in pl.queries.pairs() do
            match \exhaustive\ query
            | let pq: PreparedQuery =>
              parts.push(_FrontendMessage.parse("", pq.string,
                _ParamEncoder.oids_for(pq.params, li.codec_registry)))
              try
                parts.push(_FrontendMessage.bind("", "", pq.params,
                  li.codec_registry)?)
              else
                var i: USize = 0
                while i < pl.queries.size() do
                  try
                    pl.receiver.pg_pipeline_failed(s, i,
                      pl.queries(i)?, DataError)
                  else
                    _Unreachable()
                  end
                  i = i + 1
                end
                pl.receiver.pg_pipeline_complete(s)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
              parts.push(_FrontendMessage.describe_portal(""))
              parts.push(_FrontendMessage.execute_msg("", 0))
              parts.push(_FrontendMessage.sync())
            | let nq: NamedPreparedQuery =>
              try
                parts.push(_FrontendMessage.bind("", nq.name, nq.params,
                  li.codec_registry)?)
              else
                var i: USize = 0
                while i < pl.queries.size() do
                  try
                    pl.receiver.pg_pipeline_failed(s, i,
                      pl.queries(i)?, DataError)
                  else
                    _Unreachable()
                  end
                  i = i + 1
                end
                pl.receiver.pg_pipeline_complete(s)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
              parts.push(_FrontendMessage.describe_portal(""))
              parts.push(_FrontendMessage.execute_msg("", 0))
              parts.push(_FrontendMessage.sync())
            end
          end
          let combined = recover val
            let p: Array[Array[U8] val] ref = consume parts
            var total: USize = 0
            for part in p.values() do
              total = total + part.size()
            end
            let buf = Array[U8](total)
            var offset: USize = 0
            for part in p.values() do
              buf.copy_from(part, 0, offset, part.size())
              offset = offset + part.size()
            end
            buf
          end
          li.query_state = _PipelineInFlight.create()
          s._connection().send(consume combined)
        end

        // Set statement timeout timer if configured on the dispatched item.
        // The queue item is still at index 0 — dequeuing happens in each
        // in-flight state's on_ready_for_query.
        let timeout = match \exhaustive\ li.query_queue(0)?
        | let qry: _QueuedQuery => qry.statement_timeout
        | let prep: _QueuedPrepare => prep.statement_timeout
        | let _: _QueuedCloseStatement => None
        | let ci: _QueuedCopyIn => ci.statement_timeout
        | let co: _QueuedCopyOut => co.statement_timeout
        | let sq: _QueuedStreamingQuery => sq.statement_timeout
        | let pl: _QueuedPipeline => pl.statement_timeout
        end
        match timeout
        | let d: lori.TimerDuration =>
          match s._connection().set_timer(d)
          | let t: lori.TimerToken => li.statement_timer = t
          | let _: lori.SetTimerError => None
          end
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
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
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
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(Array[U8] val | None)] val].create()
        end
        let rd = _row_description = None

        match \exhaustive\ rd
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object = _RowsBuilder(consume rows, desc,
              li.codec_registry)?
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
          Array[Array[(Array[U8] val | None)] val] end
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
    _error = true
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
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
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
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
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows = _data_rows = recover iso
          Array[Array[(Array[U8] val | None)] val].create()
        end
        let rd = _row_description = None

        match \exhaustive\ rd
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object = _RowsBuilder(consume rows, desc,
              li.codec_registry)?
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
          Array[Array[(Array[U8] val | None)] val] end
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
    _error = true
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
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
    li.cancel_statement_timer(s)
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

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
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
    li.cancel_statement_timer(s)
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

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    // Fire-and-forget: no receiver to notify; drain_in_flight shifts
    // unconditionally in this state.
    None

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    // Fire-and-forget: no receiver to notify.
    None

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _CopyInInFlight is _QueryState
  """
  COPY ... FROM STDIN operation in progress. Uses a pull-based data flow:
  `pg_copy_ready` is called on the receiver to request each chunk. The receiver
  responds by calling `send_copy_data`, `finish_copy`, or `abort_copy` on the
  session.
  """
  var _complete_count: USize = 0
  var _error: Bool = false

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_ready(s)
    else
      _Unreachable()
    end

  fun ref send_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    data: Array[U8] val)
  =>
    s._connection().send(_FrontendMessage.copy_data(data))
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_ready(s)
    else
      _Unreachable()
    end

  fun ref finish_copy(s: Session ref) =>
    s._connection().send(_FrontendMessage.copy_done())

  fun ref abort_copy(s: Session ref, reason: String) =>
    s._connection().send(_FrontendMessage.copy_fail(reason))

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(s, msg)
    else
      _Unreachable()
    end

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    _complete_count = msg.value

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_complete(s,
          _complete_count)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
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

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)


  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(s,
          ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(s,
          SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(s,
          SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _CopyOutInFlight is _QueryState
  """
  COPY ... TO STDOUT operation in progress. The server pushes data via
  CopyData messages. Each chunk is delivered to the receiver's
  `pg_copy_data` callback. The operation completes with CopyDone followed
  by CommandComplete and ReadyForQuery.
  """
  var _complete_count: USize = 0
  var _error: Bool = false

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    // Server is ready to send data. Nothing to do — data will arrive as
    // CopyData messages.
    None

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    try
      (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_data(s,
        msg.data)
    else
      _Unreachable()
    end

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    // Data stream complete. Wait for CommandComplete + ReadyForQuery.
    None

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(s, msg)
    else
      _Unreachable()
    end

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    _complete_count = msg.value

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_complete(s,
          _complete_count)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_data_row(s: Session ref, li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
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

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)


  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(s,
          ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(s,
          SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(s,
          SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _StreamingQueryInFlight is _QueryState
  """
  Streaming query in progress. Delivers rows in windowed batches via
  `StreamingResultReceiver`. Uses Execute(max_rows > 0) + Flush to keep the
  portal alive between batches, with Sync sent only on completion or error
  to trigger ReadyForQuery. `_completing` guards against `fetch_more` and
  `close_stream` sending messages after `on_command_complete` has already
  sent Sync — the receiver may call `fetch_more()` in response to the
  final `pg_stream_batch` before `ReadyForQuery` arrives.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false
  var _completing: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
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

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let rows = _data_rows = recover iso
        Array[Array[(Array[U8] val | None)] val].create()
      end
      match _row_description
      | let desc: Array[(String, U32, U16)] val =>
        try
          let rows_object = _RowsBuilder(consume rows, desc,
            li.codec_registry)?
          sq.receiver.pg_stream_batch(s, rows_object)
        else
          _error = true
          _row_description = None
          sq.receiver.pg_stream_failed(s, sq.query, DataError)
          s._connection().send(_FrontendMessage.sync())
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref fetch_more(s: Session ref, li: _SessionLoggedIn ref) =>
    // After CommandComplete or ErrorResponse, the portal is destroyed by
    // Sync. The receiver may still call fetch_more() in response to the
    // final pg_stream_batch before ReadyForQuery arrives — silently ignore.
    if _completing or _error then return end
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let execute = _FrontendMessage.execute_msg("", sq.window_size)
      let flush_msg = _FrontendMessage.flush()
      let combined = recover val
        let total = execute.size() + flush_msg.size()
        let buf = Array[U8](total)
        buf.copy_from(execute, 0, 0, execute.size())
        buf.copy_from(flush_msg, 0, execute.size(), flush_msg.size())
        buf
      end
      s._connection().send(consume combined)
    else
      _Unreachable()
    end

  fun ref close_stream(s: Session ref) =>
    if (not _error) and (not _completing) then
      s._connection().send(_FrontendMessage.sync())
    end

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    // Final batch — deliver any remaining accumulated rows.
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let rows = _data_rows = recover iso
        Array[Array[(Array[U8] val | None)] val].create()
      end
      if rows.size() > 0 then
        match _row_description
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object = _RowsBuilder(consume rows, desc,
              li.codec_registry)?
            sq.receiver.pg_stream_batch(s, rows_object)
          else
            _error = true
            _row_description = None
            sq.receiver.pg_stream_failed(s, sq.query, DataError)
          end
        else
          _Unreachable()
        end
      end
    else
      _Unreachable()
    end
    // Send Sync to trigger ReadyForQuery and destroy the portal.
    // _completing prevents close_stream() from sending a duplicate Sync if it
    // arrives between this point and ReadyForQuery.
    _completing = true
    s._connection().send(_FrontendMessage.sync())

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_complete(s)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
      _row_description = None
      sq.receiver.pg_stream_failed(s, sq.query, msg)
    else
      _Unreachable()
    end
    // Sync is required because streaming uses Flush (not Sync) to keep the
    // portal alive. Without a pending Sync, the server waits indefinitely
    // after ErrorResponse, deadlocking the session.
    s._connection().send(_FrontendMessage.sync())

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    li.shutdown(s)

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _PipelineInFlight is _QueryState
  """
  Pipeline execution in progress. Processes N extended query cycles, one per
  pipelined query. Each cycle ends with its own Sync/ReadyForQuery. Per-query
  accumulation data is reset between cycles. `_current_index` tracks which
  query in the pipeline is currently being processed.

  Error isolation: each query has its own Sync boundary. If query 2 fails,
  the server skips to Sync2 and continues with query 3. The `_error` flag
  is per-query, reset on each ReadyForQuery.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false
  var _current_index: USize = 0

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
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

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      let rows = _data_rows = recover iso
        Array[Array[(Array[U8] val | None)] val].create()
      end
      let rd = _row_description = None

      match \exhaustive\ rd
      | let desc: Array[(String, U32, U16)] val =>
        try
          let rows_object = _RowsBuilder(consume rows, desc,
            li.codec_registry)?
          pl.receiver.pg_pipeline_result(s, _current_index,
            ResultSet(pl.queries(_current_index)?, rows_object, msg.id))
        else
          pl.receiver.pg_pipeline_failed(s, _current_index,
            pl.queries(_current_index)?, DataError)
        end
      | None =>
        if rows.size() > 0 then
          pl.receiver.pg_pipeline_failed(s, _current_index,
            pl.queries(_current_index)?, DataError)
        else
          pl.receiver.pg_pipeline_result(s, _current_index,
            RowModifying(pl.queries(_current_index)?, msg.id, msg.value))
        end
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      let rows = _data_rows = recover iso
        Array[Array[(Array[U8] val | None)] val] end
      let rd = _row_description = None

      if (rows.size() > 0) or (rd isnt None) then
        pl.receiver.pg_pipeline_failed(s, _current_index,
          pl.queries(_current_index)?, DataError)
      else
        pl.receiver.pg_pipeline_result(s, _current_index,
          SimpleResult(pl.queries(_current_index)?))
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
      _row_description = None
      pl.receiver.pg_pipeline_failed(s, _current_index,
        pl.queries(_current_index)?, msg)
    else
      _Unreachable()
    end

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    _current_index = _current_index + 1
    _error = false
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      if _current_index >= pl.queries.size() then
        // All queries processed
        pl.receiver.pg_pipeline_complete(s)
        li.query_queue.shift()?
        li.cancel_statement_timer(s)
        li.query_state = _QueryReady
        li.query_state.try_run_query(s, li)
      end
      // Otherwise, continue processing the next query cycle
    else
      _Unreachable()
    end

  fun ref on_copy_in_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    // Only the current query directly observed the violation. The remaining
    // queries are casualties of session closure and are notified with
    // SessionClosed by the subsequent drain_in_flight path.
    if not _error then
      try
        let pl = li.query_queue(0)? as _QueuedPipeline
        pl.receiver.pg_pipeline_failed(s, _current_index,
          pl.queries(_current_index)?, ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    // Only notify the current query. Remaining queries are delivered
    // SessionClosed by the subsequent drain_in_flight path.
    if not _error then
      try
        let pl = li.query_queue(0)? as _QueuedPipeline
        pl.receiver.pg_pipeline_failed(s, _current_index,
          pl.queries(_current_index)?, SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      // Notify current query if not already error-notified
      if not _error then
        pl.receiver.pg_pipeline_failed(s, _current_index,
          pl.queries(_current_index)?, SessionClosed)
      end
      // Notify remaining queries
      var i = _current_index + 1
      while i < pl.queries.size() do
        pl.receiver.pg_pipeline_failed(s, i,
          pl.queries(i)?, SessionClosed)
        i = i + 1
      end
      pl.receiver.pg_pipeline_complete(s)
    else
      _Unreachable()
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

interface _SessionState
  fun on_connected(s: Session ref)
    """
    Called when a connection is established with the server.
    """
  fun ref on_tls_ready(s: Session ref)
    """
    Called when a TLS handshake initiated by start_tls() completes.
    """
  fun ref on_authentication_ok(s: Session ref)
    """
    Called when we successfully authenticate with the server.
    """
  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
    """
    Called when the session fails to reach the ready state — any transport
    failure, TLS failure, unsupported authentication method, server
    protocol violation, or server rejection during startup. Fires
    pg_session_connection_failed and transitions to _SessionClosed.
    """
  fun ref on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
    """
    Called if the server requests we autheticate using the Postgres MD5
    password scheme.
    """
  fun ref on_authentication_cleartext_password(s: Session ref)
    """
    Called if the server requests we authenticate using a cleartext password.
    """
  fun ref on_timer(s: Session ref, token: lori.TimerToken)
    """
    A statement timeout timer fired. Like `cancel`, this should never be an
    illegal state — it should be silently ignored when not applicable.
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

  fun ref on_protocol_violation(s: Session ref)
    """
    Called when the server sends data that violates the wire protocol —
    either unparseable bytes, a well-formed message of a type invalid in
    the current state, or an unexpected byte during SSL negotiation. The
    session cannot recover; implementations deliver the failure to the user
    through the callback appropriate for the current state and transition
    to `_SessionClosed`.
    """

  fun ref on_closed(s: Session ref)
    """
    Called when lori reports that the TCP connection is closed. State
    implementations deliver the failure to the user through the callback
    appropriate for the current state and transition to `_SessionClosed`.
    Implementations must be idempotent with user-initiated close — lori
    fires `_on_closed` after any `hard_close()`, including closes this
    session itself initiated.
    """

  fun ref on_received(s: Session ref, data: Array[U8] iso)
    """
    Called when we receive data from the server.
    """
  
  fun ref execute(s: Session ref, query: Query, receiver: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a query execution.
    """

  fun ref prepare(s: Session ref, name: String, sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a named statement preparation.
    """

  fun ref close_statement(s: Session ref, name: String)
    """
    Called when a client requests closing a named prepared statement.
    """

  fun ref copy_in(s: Session ref, sql: String, receiver: CopyInReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a COPY ... FROM STDIN operation.
    """

  fun ref send_copy_data(s: Session ref, data: Array[U8] val)
    """
    Called when a client sends a chunk of COPY data.
    """

  fun ref finish_copy(s: Session ref)
    """
    Called when a client signals completion of the COPY data stream.
    """

  fun ref abort_copy(s: Session ref, reason: String)
    """
    Called when a client aborts the COPY operation.
    """

  fun ref on_copy_in_response(s: Session ref, msg: _CopyInResponseMessage)
    """
    Called when the server responds to a COPY FROM STDIN query with a
    CopyInResponse message, indicating it is ready to receive data.
    """

  fun ref copy_out(s: Session ref, sql: String, receiver: CopyOutReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a COPY ... TO STDOUT operation.
    """

  fun ref on_copy_out_response(s: Session ref, msg: _CopyOutResponseMessage)
    """
    Called when the server responds to a COPY TO STDOUT query with a
    CopyOutResponse message, indicating it is ready to send data.
    """

  fun ref on_copy_data(s: Session ref, msg: _CopyDataMessage)
    """
    Called when the server sends a CopyData message during a COPY TO STDOUT
    operation, containing a chunk of the exported data.
    """

  fun ref on_copy_done(s: Session ref)
    """
    Called when the server sends a CopyDone message, indicating the end of
    the COPY TO STDOUT data stream.
    """

  fun ref on_portal_suspended(s: Session ref)
    """
    Called when the server sends a PortalSuspended message during a streaming
    query, indicating more rows are available for the current portal.
    """

  fun ref stream(s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32, receiver: StreamingResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a streaming query execution.
    """

  fun ref fetch_more(s: Session ref)
    """
    Called when a client requests the next batch of streaming rows.
    """

  fun ref close_stream(s: Session ref)
    """
    Called when a client requests early termination of a streaming query.
    """

  fun ref pipeline(s: Session ref,
    queries: Array[(PreparedQuery | NamedPreparedQuery)] val,
    receiver: PipelineReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a pipelined query execution.
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
    Called when the server sends an ErrorResponse. During pre-ready startup
    states, this routes through `_ConnectionFailureReasonFromError` to fire
    `pg_session_connection_failed`. During the logged-in state, this flows
    into `_QueryState` error handling so the error is delivered to the
    failing query's receiver.
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

  fun ref on_notice(s: Session ref, msg: NoticeResponseMessage)
    """
    Called when the server sends a NoticeResponse (non-fatal informational
    message). Can arrive in any connected state, including during
    authentication.
    """

  fun ref on_parameter_status(s: Session ref, msg: _ParameterStatusMessage)
    """
    Called when the server sends a ParameterStatus message reporting a runtime
    parameter's current value. Can arrive during startup and after SET commands.
    """

  fun ref on_notification(s: Session ref, msg: _NotificationResponseMessage)
    """
    Called when the server sends a LISTEN/NOTIFY notification.
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
    match \exhaustive\ ssl_mode()
    | SSLDisabled =>
      s.state = _SessionConnected(notify(), database_connect_info(),
        codec_registry())
      notify().pg_session_connected(s)
      _send_startup_message(s)
    | let pref: SSLPreferred =>
      _start_ssl_negotiation(s, pref.ctx, true)
    | let req: SSLRequired =>
      _start_ssl_negotiation(s, req.ctx, false)
    end

  fun _start_ssl_negotiation(s: Session ref, ctx: SSLContext val,
    fallback_on_refusal: Bool)
  =>
    // Set buffer_until(1) BEFORE sending SSLRequest so lori delivers exactly
    // one byte per _on_received call. Any MITM-injected bytes stay in
    // lori's internal buffer, causing start_tls() to return
    // StartTLSNotReady (CVE-2021-23222 mitigation).
    match \exhaustive\ lori.MakeBufferSize(1)
    | let e: lori.BufferSize => s._connection().buffer_until(e)
    else
      _Unreachable()
    end
    let st = _SessionSSLNegotiating(
      notify(), database_connect_info(), ctx, host(),
      fallback_on_refusal, codec_registry())
    s.state = st
    st.send_ssl_request(s)

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    notify().pg_session_connection_failed(s, reason)
    notify().pg_session_shutdown(s)
    s.state = _SessionClosed

  fun _send_startup_message(s: Session ref) =>
    let dci = database_connect_info()
    let msg = _FrontendMessage.startup(dci.user, dci.database)
    s._connection().send(msg)

  fun database_connect_info(): DatabaseConnectInfo
  fun ssl_mode(): SSLMode
  fun host(): String
  fun notify(): SessionStatusNotify
  fun codec_registry(): CodecRegistry

trait _NotConnectableState
  """
  A session that if it gets messages related to connect to a server, then
  something has gone wrong with the state machine.
  """
  fun on_connected(s: Session ref) =>
    _IllegalState()

trait _ConnectedState is _NotConnectableState
  """
  A connected session. Connected sessions are not connectable as they have
  already been connected.
  """
  fun ref on_notice(s: Session ref, msg: NoticeResponseMessage) =>
    notify().pg_notice(s, msg)

  fun ref on_parameter_status(s: Session ref,
    msg: _ParameterStatusMessage)
  =>
    notify().pg_parameter_status(s,
      ParameterStatus(msg.name, msg.value))

  fun ref on_tls_ready(s: Session ref) =>
    _IllegalState()

  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    readbuf().append(consume data)
    process_responses(s)

  fun ref process_responses(s: Session ref) =>
    _ResponseMessageParser(s, readbuf())

  fun ref on_timer(s: Session ref, token: lori.TimerToken) =>
    None

  fun ref cancel(s: Session ref) =>
    None

  fun ref send_copy_data(s: Session ref, data: Array[U8] val) =>
    None

  fun ref finish_copy(s: Session ref) =>
    None

  fun ref abort_copy(s: Session ref, reason: String) =>
    None

  fun ref fetch_more(s: Session ref) =>
    None

  fun ref close_stream(s: Session ref) =>
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
  fun ref on_notice(s: Session ref, msg: NoticeResponseMessage) =>
    _IllegalState()

  fun ref on_parameter_status(s: Session ref,
    msg: _ParameterStatusMessage)
  =>
    _IllegalState()

  fun ref on_tls_ready(s: Session ref) =>
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

  fun ref on_timer(s: Session ref, token: lori.TimerToken) =>
    None

  fun ref cancel(s: Session ref) =>
    None

  fun ref send_copy_data(s: Session ref, data: Array[U8] val) =>
    None

  fun ref finish_copy(s: Session ref) =>
    None

  fun ref abort_copy(s: Session ref, reason: String) =>
    None

  fun ref fetch_more(s: Session ref) =>
    None

  fun ref close_stream(s: Session ref) =>
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
    s.state = _SessionLoggedIn(notify(), readbuf(), codec_registry())
    notify().pg_session_authenticated(s)

  fun ref on_connection_failed(s: Session ref,
    reason: ConnectionFailureReason)
  =>
    notify().pg_session_connection_failed(s, reason)
    shutdown(s)

  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage) =>
    on_connection_failed(s, _ConnectionFailureReasonFromError(msg))

  fun ref on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    let md5_password = _MD5Password(user(), password(), msg.salt)
    let reply = _FrontendMessage.password(md5_password)
    s._connection().send(reply)

  fun ref on_authentication_cleartext_password(s: Session ref) =>
    let reply = _FrontendMessage.password(password())
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
      on_connection_failed(s, UnsupportedAuthenticationMethod)
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
        notify(), readbuf(), nonce, client_first_bare, password(),
        codec_registry())
    else
      shutdown(s)
    end

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    on_protocol_violation(s)

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    on_protocol_violation(s)

  fun ref on_protocol_violation(s: Session ref) =>
    on_connection_failed(s, ProtocolViolation)

  fun user(): String
  fun password(): String
  fun ref readbuf(): Reader
  fun notify(): SessionStatusNotify
  fun codec_registry(): CodecRegistry

trait _NotAuthenticableState
  """
  A session that isn't eligible to be authenticated. Only connected sessions
  that haven't yet been authenticated are eligible to be authenticated. A
  server-sent authentication message in such a state is a protocol
  violation — routed through `on_protocol_violation`, which each concrete
  state handles appropriately (panic for states where the parser never runs,
  failure delivery for states where it does).
  """
  fun ref on_protocol_violation(s: Session ref)

  fun ref on_authentication_ok(s: Session ref) =>
    on_protocol_violation(s)

  fun ref on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    on_protocol_violation(s)

  fun ref on_authentication_cleartext_password(s: Session ref) =>
    on_protocol_violation(s)

  fun ref on_authentication_sasl(s: Session ref,
    msg: _AuthenticationSASLMessage)
  =>
    on_protocol_violation(s)

  fun ref on_authentication_sasl_continue(s: Session ref,
    msg: _AuthenticationSASLContinueMessage)
  =>
    on_protocol_violation(s)

  fun ref on_authentication_sasl_final(s: Session ref,
    msg: _AuthenticationSASLFinalMessage)
  =>
    on_protocol_violation(s)

trait _AuthenticatedState is (_ConnectedState & _NotAuthenticableState)
  """
  A connected and authenticated session. Connected sessions are not connectable
  as they have already been connected. Authenticated sessions are not
  authenticable as they have already been authenticated.
  """


trait _NotAuthenticated
  """
  A session that has yet to be authenticated. Before being authenticated,
  query-related messages should not be received from the server. Such a
  message is a protocol violation — routed through `on_protocol_violation`,
  which each concrete state handles appropriately (panic for states where
  the parser never runs, failure delivery for states where it does).
  """
  fun ref on_protocol_violation(s: Session ref)

  fun ref on_notification(s: Session ref, msg: _NotificationResponseMessage) =>
    on_protocol_violation(s)

  fun ref on_backend_key_data(s: Session ref, msg: _BackendKeyDataMessage) =>
    on_protocol_violation(s)

  fun ref on_command_complete(s: Session ref, msg: _CommandCompleteMessage) =>
    on_protocol_violation(s)

  fun ref on_data_row(s: Session ref, msg: _DataRowMessage) =>
    on_protocol_violation(s)

  fun ref on_empty_query_response(s: Session ref) =>
    on_protocol_violation(s)

  fun ref on_error_response(s: Session ref, msg: ErrorResponseMessage) =>
    on_protocol_violation(s)

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    on_protocol_violation(s)

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage) =>
    on_protocol_violation(s)

  fun ref on_copy_in_response(s: Session ref, msg: _CopyInResponseMessage) =>
    on_protocol_violation(s)

  fun ref on_copy_out_response(s: Session ref,
    msg: _CopyOutResponseMessage)
  =>
    on_protocol_violation(s)

  fun ref on_copy_data(s: Session ref, msg: _CopyDataMessage) =>
    on_protocol_violation(s)

  fun ref on_copy_done(s: Session ref) =>
    on_protocol_violation(s)

  fun ref on_portal_suspended(s: Session ref) =>
    on_protocol_violation(s)
