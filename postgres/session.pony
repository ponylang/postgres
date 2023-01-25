use "buffered"
use lori = "lori"

actor Session is lori.TCPClientActor
  let notify: SessionStatusNotify
  let host: String
  let service: String
  // TODO SEAN move these 3 into state object(s)
  let user: String
  let password: String
  let database: String

  // TODO SEAN move readbuf into state object(s)
  let readbuf: Reader = Reader
  var state: _SessionState = _SessionUnopened

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
    notify = notify'
    host = host'
    service = service'
    user = user'
    password = password'
    database = database'

    _tcp_connection = lori.TCPConnection.client(auth', host, service, "", this)

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
    // TODO SEAN: because a user can send this message when the actor is in
    // any state, we need to have a call other than shutdown which will in
    // states where a close isn't valid, simply ignore it. and for those were
    // it is valid, call shutdown. this needs to go on each of our states.
    // probably via closeable and uncloseable traits.
    state.shutdown(this)

  be _process_again() =>
    _process_responses()

  fun ref _on_connected() =>
    state.on_connected(this)

  fun ref _on_connection_failure() =>
    state.on_failure(this)

  fun ref _on_received(data: Array[U8] iso) =>
    state.on_received(this, consume data)
    _process_responses()

  fun ref _process_responses() =>
    """
    Handles all messages regardless of whether they are valid for our current
    state. The `s.state` calls that result will handle if a message isn't legal
    for our current state.
    """
    // TODO: move all this to its own primitive that takes a
    // session and readbuffer.
    // only our state objects that can parse messages will use the
    // primitive in their process responses handler.
    // others will do illegal state.
    // read buffer will be moved into state objects and reference
    // past as part of transition from connected to logged in, both
    // of which will need it.
    try
      match _ResponseParser(readbuf)?
      | let msg: _AuthenticationMD5PasswordMessage =>
        state.on_authentication_md5_password(this, msg)
      | _AuthenticationOkMessage =>
        state.on_authentication_ok(this)
      | let err: _ErrorResponseMessage =>
        if (err.code == _ErrorCode.invalid_password())
          or (err.code == _ErrorCode.invalid_authentication_specification())
        then
          let reason = if err.code == _ErrorCode.invalid_password() then
            InvalidPassword
          else
            InvalidAuthenticationSpecification
          end

          state.on_authentication_failed(this, reason)
          return
        end
      | let msg: _ReadyForQueryMessage =>
        state.on_ready_for_query(this, msg)
      | None =>
        // No complete message was found. Stop parsing for now.
        return
      end
    else
      // An unrecoverable error was encountered while parsing. Once that
      // happens, there's no way we are going to be able to figure out how
      // to get the responses back into an understandable state. The only
      // thing we can do is shut this session down.

      state.shutdown(this)
      return
    end

    _process_again()

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

// Possible session states
class ref _SessionUnopened is _ConnectableState
  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SesssionNeverOpened)

class ref _SessionClosed is (_NotConnectableState & _UnconnectedState)
  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SessionClosed)

class ref _SessionConnected is _AuthenticableState
  fun ref execute(s: Session ref, q: SimpleQuery, r: ResultReceiver) =>
    r.pg_query_failed(q, SessionNotAuthenticated)

class _SessionLoggedIn is _AuthenticatedState
  var _queryable: Bool = false
  var _query_in_flight: Bool = false
  let _query_queue: Array[(SimpleQuery, ResultReceiver)] = _query_queue.create()

  new ref create() =>
    None

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    @printf("on_ready_for_query\n".cstring())
    if msg.idle() then
      // TODO SEAN this isn't correct as it assumes success which might not
      // happened. We need a state machine for "in flight query".
      if _query_in_flight then
        try
          (let query, let receiver) = _query_queue.shift()?
          receiver.pg_query_result(Result(query))
        else
          // TODO SEAN unreachable
          None
        end
      end
      _queryable = true
      _run_query(s)
    else
      _queryable = false
    end

  fun ref execute(s: Session ref,
    query: SimpleQuery,
    receiver: ResultReceiver)
  =>
    @printf("execute received\n".cstring())
    _query_queue.push((query, receiver))
    _run_query(s)

  fun ref _run_query(s: Session ref) =>
    try
      if _queryable and (_query_queue.size() > 0) then
        @printf("running query\n".cstring())
        _queryable = false
        _query_in_flight = true
        (let query, _) = _query_queue(0)?
        let msg = _Message.query(query.string)
        s._connection().send(msg)
      else
        @printf("not running query\n".cstring())
      end
    else
      // TODO SEAN unreachable
      None
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
  fun on_authentication_ok(s: Session ref)
    """
    Called when we successfully authenticate with the server.
    """
  fun on_authentication_failed(
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
  fun shutdown(s: Session ref)
    """
    Called when we are shutting down the session.
    """
  fun on_received(s: Session ref, data: Array[U8] iso)
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

trait _ConnectableState is _UnconnectedState
  """
  An unopened session that can be connected to a server.
  """
  fun on_connected(s: Session ref) =>
    s.state = _SessionConnected
    s.notify.pg_session_connected(s)
    _send_startup_message(s)

  fun on_failure(s: Session ref) =>
    s.state = _SessionClosed
    s.notify.pg_session_connection_failed(s)

  fun _send_startup_message(s: Session ref) =>
    let msg = _Message.startup(s.user, s.database)
    s._connection().send(msg)

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
  fun on_received(s: Session ref, data: Array[U8] iso) =>
    s.readbuf.append(consume data)

  fun shutdown(s: Session ref) =>
    s.state = _SessionClosed
    s.readbuf.clear()
    s._connection().close()
    s.notify.pg_session_shutdown(s)

trait _UnconnectedState is (_NotAuthenticableState & _NotAuthenticated)
  """
  A session that isn't connected. Either because it was never opened or because
  it has been closed. Unconnected sessions are not eligible to be authenticated
  and receiving an authentication event while unconnected is an error.
  """
  fun on_received(s: Session ref, data: Array[U8] iso) =>
    // It is possible we will continue to receive data after we have closed
    // so this isn't an invalid state. We should silently drop the data. If
    // "not yet opened" and "closed" were different states, rather than a single
    // "unconnected" then we would want to call illegal state if `on_received`
    // was called when the state was "not yet opened".
    None

  fun shutdown(s: Session ref) =>
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
  fun on_authentication_ok(s: Session ref) =>
    s.state = _SessionLoggedIn
    s.notify.pg_session_authenticated(s)

  fun on_authentication_failed(s: Session ref, r: AuthenticationFailureReason) =>
    s.notify.pg_session_authentication_failed(s, r)
    shutdown(s)

  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    let md5_password = _MD5Password(s.user, s.password, msg.salt)
    let reply = _Message.password(md5_password)
    s._connection().send(reply)

trait _NotAuthenticableState
  """
  A session that isn't eligible to be authenticated. Only connected sessions
  that haven't yet been authenticated are eligible to be authenticated.
  """
  fun on_authentication_ok(s: Session ref) =>
    _IllegalState()

  fun on_authentication_failed(
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
  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    _IllegalState()
