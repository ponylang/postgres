use "buffered"
use "encode/base64"
use lori = "lori"
use "ssl/crypto"
use "ssl/net"

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

  fun ref on_connection_failed(
    s: Session ref,
    reason: ConnectionFailureReason)
    """
    Called when the session fails to reach the ready state — any transport
    failure, TLS failure, unsupported authentication method, server
    protocol violation, or server rejection during startup. Fires
    pg_session_connection_failed and transitions to _SessionClosed.
    """

  fun ref on_authentication_md5_password(
    s: Session ref,
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

  fun ref on_timer_failure(s: Session ref)
    """
    Lori reported that the statement timeout timer's ASIO event subscription
    failed. Like `on_timer`, this should never be an illegal state — it
    should be silently ignored when not applicable. `_SessionLoggedIn`
    (the only state that arms the statement timer) rearms using the
    in-flight operation's original duration; every other state is a no-op.
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

  fun ref execute(
    s: Session ref,
    query: Query,
    receiver: ResultReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a query execution.
    """

  fun ref prepare(
    s: Session ref,
    name: String,
    sql: String,
    receiver: PrepareReceiver,
    statement_timeout: (lori.TimerDuration | None) = None)
    """
    Called when a client requests a named statement preparation.
    """

  fun ref close_statement(s: Session ref, name: String)
    """
    Called when a client requests closing a named prepared statement.
    """

  fun ref copy_in(
    s: Session ref,
    sql: String,
    receiver: CopyInReceiver,
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

  fun ref copy_out(
    s: Session ref,
    sql: String,
    receiver: CopyOutReceiver,
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

  fun ref stream(
    s: Session ref,
    query: (PreparedQuery | NamedPreparedQuery),
    window_size: U32,
    receiver: StreamingResultReceiver,
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

  fun ref pipeline(
    s: Session ref,
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
      s.state =
        _SessionConnected(
          notify(), database_connect_info(), codec_registry())
      notify().pg_session_connected(s)
      _send_startup_message(s)
    | let pref: SSLPreferred =>
      _start_ssl_negotiation(s, pref.ctx, true)
    | let req: SSLRequired =>
      _start_ssl_negotiation(s, req.ctx, false)
    end

  fun _start_ssl_negotiation(
    s: Session ref,
    ctx: SSLContext val,
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
    let st =
      _SessionSSLNegotiating(
        notify(),
        database_connect_info(),
        ctx,
        host(),
        fallback_on_refusal,
        codec_registry())
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
    notify().pg_parameter_status(
      s,
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

  fun ref on_timer_failure(s: Session ref) =>
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

  fun ref on_timer_failure(s: Session ref) =>
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
    // A server that completes startup without any authentication challenge
    // is offering the weakest posture; the default `AuthRequireSCRAM`
    // policy rejects it. See `AuthRequirement`.
    match \exhaustive\ s.server_connect_info().auth_requirement
    | AuthRequireSCRAM =>
      on_connection_failed(s, AuthenticationMethodRejected)
    | AllowAnyAuth =>
      s.state = _SessionLoggedIn(notify(), readbuf(), codec_registry())
      notify().pg_session_authenticated(s)
    end

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
    // Enforces `AuthRequirement` before replying with the MD5-hashed
    // password — under `AuthRequireSCRAM`, no credential bytes reach the
    // wire.
    match \exhaustive\ s.server_connect_info().auth_requirement
    | AuthRequireSCRAM =>
      on_connection_failed(s, AuthenticationMethodRejected)
    | AllowAnyAuth =>
      let md5_password = _MD5Password(user(), password(), msg.salt)
      let reply = _FrontendMessage.password(md5_password)
      s._connection().send(reply)
    end

  fun ref on_authentication_cleartext_password(s: Session ref) =>
    // Enforces `AuthRequirement` before replying with the password —
    // under `AuthRequireSCRAM`, no credential bytes reach the wire.
    match \exhaustive\ s.server_connect_info().auth_requirement
    | AuthRequireSCRAM =>
      on_connection_failed(s, AuthenticationMethodRejected)
    | AllowAnyAuth =>
      let reply = _FrontendMessage.password(password())
      s._connection().send(reply)
    end

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
      s.state =
        _SessionSCRAMAuthenticating(
          notify(),
          readbuf(),
          nonce,
          client_first_bare,
          password(),
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
