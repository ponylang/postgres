use "buffered"
use lori = "lori"

actor Session is lori.TCPClientActor
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
    r.pg_query_failed(q, SesssionNeverOpened)

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

// TODO SEAN
// some of these callbacks assume we are "in a query" and we should be
// blowing up if we aren't. An additional level of state machine for query is
// probably needed.
class _SessionLoggedIn is _AuthenticatedState
  var _queryable: Bool = false
  var _query_in_flight: Bool = false
  let _query_queue: Array[(SimpleQuery, ResultReceiver)] = _query_queue.create()
  let _notify: SessionStatusNotify
  let _readbuf: Reader
  var _data_rows: Array[Array[(String|None)] val] iso
  var _row_description: Array[(String, U32)] val

  new ref create(notify': SessionStatusNotify, readbuf': Reader) =>
    _notify = notify'
    _readbuf = readbuf'
    _data_rows = recover iso Array[Array[(String|None)] val] end
    _row_description = recover val Array[(String, U32)] end

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    if msg.idle() then
      if _query_in_flight then
        // If there was a query in flight, we are now done with it.
        try
          _query_queue.shift()?
        end
        _query_in_flight = false
      end
      _queryable = true
      _run_query(s)
    else
      _queryable = false
    end

  fun ref on_command_complete(s: Session ref, msg: _CommandCompleteMessage) =>
    """
    A command has completed, that might mean the active is query is done. At
    this point we don't know. We grab the active query from the head of the
    query queue while leaving it in place and inform the receiver of a success
    for at least one part of the query.
    """
    // TODO SEAN should check that a query is in flight
    try
      (let query, let receiver) = _query_queue(0)?
      let rows = _data_rows = recover iso
        Array[Array[(String|None)] val].create()
      end

      try
        let rows_object = _RowsBuilder(consume rows, _row_description)?
        receiver.pg_query_result(Result(query, rows_object))
      else
        receiver.pg_query_failed(query, FreeCandy)
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(s: Session ref, msg: _ErrorResponseMessage) =>
    // TODO SEAN we should verify query in flight
    try
      (let query, let receiver) = _query_queue(0)?
      receiver.pg_query_failed(query, FreeCandy)
    else
      _Unreachable()
    end

  fun ref on_data_row(s: Session ref, msg: _DataRowMessage) =>
    // TODO SEAN we should verify query in flight
    _data_rows.push(msg.columns)

  fun ref execute(s: Session ref,
    query: SimpleQuery,
    receiver: ResultReceiver)
  =>
    _query_queue.push((query, receiver))
    _run_query(s)

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage) =>
    // TODO SEAN we should verify query in flight
    // TODO we should very that only get 1 of these per in flight query
    _row_description = msg.columns

  fun ref _run_query(s: Session ref) =>
    try
      if _queryable and (_query_queue.size() > 0) then
        (let query, _) = _query_queue(0)?
        _queryable = false
        _query_in_flight = true
        let msg = _Message.query(query.string)
        s._connection().send(msg)
      end
    else
      _Unreachable()
    end

  fun ref on_shutdown(s: Session ref) =>
    _readbuf.clear()
    // TODO SEAN we need to test this happens correctly. Sending the
    // notification of "failure" for open queries on shutdown.
    // To do this, we need a dummy server that will accept the incoming query
    // messages but never return a result thereby guaranteeing that any queries
    // we send for the test should get a query failed AFTER a close is sent.
    for queue_item in _query_queue.values() do
      (let query, let receiver) = queue_item
      receiver.pg_query_failed(query, SessionClosed)
    end
    _query_queue.clear()

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

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

  fun ref on_error_response(s: Session ref, msg: _ErrorResponseMessage)
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
    let msg = _Message.startup(user(), database())
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
    let reply = _Message.password(md5_password)
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

  fun ref on_error_response(s: Session ref, msg: _ErrorResponseMessage) =>
    _IllegalState()

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    _IllegalState()

  fun ref on_row_description(s: Session ref, msg: _RowDescriptionMessage) =>
    _IllegalState()
