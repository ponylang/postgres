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
    // TODO SEAN: because a user can send this message when the actor is in
    // any state, we need to have a call other than shutdown which will in
    // states where a close isn't valid, simply ignore it. and for those were
    // it is valid, call shutdown. this needs to go on each of our states.
    // probably via closeable and uncloseable traits.
    state.shutdown(this)

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

  fun user(): String =>
    _user

  fun password(): String =>
    _password

  fun ref readbuf(): Reader =>
    _readbuf

  fun notify(): SessionStatusNotify =>
    _notify

class _SessionLoggedIn is _AuthenticatedState
  var _queryable: Bool = false
  var _query_in_flight: Bool = false
  let _query_queue: Array[(SimpleQuery, ResultReceiver)] = _query_queue.create()
  let _notify: SessionStatusNotify
  let _readbuf: Reader

  new ref create(notify': SessionStatusNotify, readbuf': Reader) =>
    _notify = notify'
    _readbuf = readbuf'

  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
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
    _query_queue.push((query, receiver))
    _run_query(s)

  fun ref _run_query(s: Session ref) =>
    try
      if _queryable and (_query_queue.size() > 0) then
        _queryable = false
        _query_in_flight = true
        (let query, _) = _query_queue(0)?
        let msg = _Message.query(query.string)
        s._connection().send(msg)
      end
    else
      // TODO SEAN unreachable
      None
    end

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
  fun ref process_responses(s: Session ref) =>
    """
    Called to process responses we've received from the server after the data
    has been parsed into messages.
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

  fun ref shutdown(s: Session ref) =>
    s.state = _SessionClosed
    readbuf().clear()
    s._connection().close()
    notify().pg_session_shutdown(s)

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
  fun ref on_ready_for_query(s: Session ref, msg: _ReadyForQueryMessage) =>
    _IllegalState()
