use "buffered"
use lori = "lori"

actor Session is (lori.TCPConnectionActor & lori.ClientLifecycleEventReceiver)
  var state: _SessionState
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(
    auth': lori.TCPConnectAuth,
    notify': SessionStatusNotify,
    host': String,
    service': String,
    user': String,
    password': String,
    database': String)
  =>
    state = _SessionUnopened(notify', user', password', database')

    _tcp_connection = lori.TCPConnection.client(auth',
      host',
      service',
      "",
      this,
      this)

  be execute(query: SimpleQuery, receiver: ResultReceiver) =>
    """
    Execute a query.
    """
    state.execute(this, query, receiver)

  be close() =>
    """
    Hard closes the connection. Terminates as soon as possible without waiting
    for outstanding queries to finish.
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

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _next_lifecycle_event_receiver(): None =>
    None

// Possible session states
class ref _SessionUnopened is _ConnectableState
  let _notify: SessionStatusNotify
  let _user: String
  let _password: String
  let _database: String

  new ref create(notify': SessionStatusNotify,
    user': String,
    password': String,
    database': String)
  =>
    _notify = notify'
    _user = user'
    _password = password'
    _database = database'

  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SessionNeverOpened)

  fun user(): String =>
    _user

  fun password(): String =>
    _password

  fun database(): String =>
    _database

  fun notify(): SessionStatusNotify =>
    _notify

class ref _SessionClosed is (_NotConnectableState & _UnconnectedState)
  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SessionClosed)

class ref _SessionConnected is _AuthenticableState
  let _notify: SessionStatusNotify
  let _user: String
  let _password: String
  let _database: String
  let _readbuf: Reader = _readbuf.create()

  new ref create(notify': SessionStatusNotify,
    user': String,
    password': String,
    database': String)
  =>
    _notify = notify'
    _user = user'
    _password = password'
    _database = database'

  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SessionNotAuthenticated)

  fun ref on_shutdown(s: Session ref) =>
    _readbuf.clear()

  fun user(): String =>
    _user

  fun password(): String =>
    _password

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

class _SessionLoggedIn is _AuthenticatedState
  """
  An authenticated session ready to execute queries. Query execution is
  managed by a sub-state machine (`_QueryState`) that tracks whether a query
  is in flight, what protocol is active, and owns per-query accumulation data.
  """
  // query_queue and query_state are not underscore-prefixed because the
  // _QueryState implementations need to access them, and Pony private fields
  // are type-private.
  let query_queue: Array[(SimpleQuery, ResultReceiver)] =
    query_queue.create()
  var query_state: _QueryState
  let _notify: SessionStatusNotify
  let _readbuf: Reader

  new ref create(notify': SessionStatusNotify, readbuf': Reader) =>
    _notify = notify'
    _readbuf = readbuf'
    query_state = _QueryNotReady

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    query_state.on_ready_for_query(s, this, msg)

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

  fun ref execute(s: Session ref,
    query: SimpleQuery,
    receiver: ResultReceiver)
  =>
    query_queue.push((query, receiver))
    query_state.try_run_query(s, this)

  fun ref on_shutdown(s: Session ref) =>
    _readbuf.clear()
    for queue_item in query_queue.values() do
      (let query, let receiver) = queue_item
      receiver.pg_query_failed(query, SessionClosed)
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
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref,
    msg: _ReadyForQueryMessage)
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
  authentication and the state after a non-idle ReadyForQuery (failed
  transaction).
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref,
    msg: _ReadyForQueryMessage)
  =>
    if msg.idle() then
      li.query_state = _QueryReady
      li.query_state.try_run_query(s, li)
    end

class _QueryReady is _QueryNoQueryInFlight
  """
  Server is idle and ready to accept a query. If the queue is non-empty,
  `try_run_query` immediately transitions to an in-flight state.

  ReadyForQuery while already ready indicates a protocol anomaly — the
  server only sends ReadyForQuery in response to a query cycle or Sync.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref,
    msg: _ReadyForQueryMessage)
  =>
    li.shutdown(s)

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      if li.query_queue.size() > 0 then
        (let query, _) = li.query_queue(0)?
        li.query_state = _SimpleQueryInFlight.create()
        s._connection().send(_FrontendMessage.query(query.string))
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

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref,
    msg: _ReadyForQueryMessage)
  =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    if msg.idle() then
      li.query_state = _QueryReady
      li.query_state.try_run_query(s, li)
    else
      li.query_state = _QueryNotReady
    end

  fun ref on_command_complete(s: Session ref, li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      (let query, let receiver) = li.query_queue(0)?
      let rows = _data_rows = recover iso
        Array[Array[(String|None)] val].create()
      end
      let rd = _row_description = None

      match rd
      | let desc: Array[(String, U32)] val =>
        try
          let rows_object = _RowsBuilder(consume rows, desc)?
          receiver.pg_query_result(ResultSet(query, rows_object, msg.id))
        else
          receiver.pg_query_failed(query, DataError)
        end
      | None =>
        if rows.size() > 0 then
          receiver.pg_query_failed(query, DataError)
        else
          receiver.pg_query_result(RowModifying(query, msg.id, msg.value))
        end
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      (let query, let receiver) = li.query_queue(0)?
      let rows = _data_rows = recover iso
        Array[Array[(String|None)] val] end
      let rd = _row_description = None

      if (rows.size() > 0) or (rd isnt None) then
        receiver.pg_query_failed(query, DataError)
      else
        receiver.pg_query_result(SimpleResult(query))
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(s: Session ref, li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    try
      (let query, let receiver) = li.query_queue(0)?
      _data_rows = recover iso Array[Array[(String|None)] val] end
      _row_description = None
      receiver.pg_query_failed(query, msg)
    else
      _Unreachable()
    end

interface _SessionState
  fun on_connected(s: Session ref)
    """
    Called when a connection is established with the server.
    """
  fun on_failure(s: Session ref)
    """
    Called if we fail to establish a connection with the server.
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
  fun ref execute(s: Session ref, query: SimpleQuery, receiver: ResultReceiver)
    """
    Called when a client requests a query execution.
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

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage)
    """
    Called when a row description is receivedfrom the server.
    """

trait _ConnectableState is _UnconnectedState
  """
  An unopened session that can be connected to a server.
  """
  fun on_connected(s: Session ref) =>
    s.state = _SessionConnected(notify(), user(), password(), database())
    notify().pg_session_connected(s)
    _send_startup_message(s)

  fun on_failure(s: Session ref) =>
    s.state = _SessionClosed
    notify().pg_session_connection_failed(s)

  fun _send_startup_message(s: Session ref) =>
    let msg = _FrontendMessage.startup(user(), database())
    s._connection().send(msg)

  fun user(): String
  fun password(): String
  fun database(): String
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
  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    readbuf().append(consume data)
    process_responses(s)

  fun ref process_responses(s: Session ref) =>
    _ResponseMessageParser(s, readbuf())

  fun ref close(s: Session ref) =>
    shutdown(s)

  fun ref shutdown(s: Session ref) =>
    on_shutdown(s)
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
  fun ref on_received(s: Session ref, data: Array[U8] iso) =>
    // It is possible we will continue to receive data after we have closed
    // so this isn't an invalid state. We should silently drop the data. If
    // "not yet opened" and "closed" were different states, rather than a single
    // "unconnected" then we would want to call illegal state if `on_received`
    // was called when the state was "not yet opened".
    None

  fun ref process_responses(s: Session ref) =>
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
