use "buffered"
use lori = "lori"

actor Session is lori.TCPClientActor
  let notify: SessionStatusNotify
  let host: String
  let service: String
  let user: String
  let password: String
  let database: String

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

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_connected() =>
    state.on_connected(this)

  fun ref _on_connection_failure() =>
    state.on_failure(this)

  fun ref _on_received(data: Array[U8] iso) =>
    state.on_received(this, consume data)

// Possible session states
primitive _SessionUnopened is _ConnectableState
primitive _SessionClosed is (_NotConnectableState & _UnconnectedState)
primitive _SessionConnected is _AuthenticableState
primitive _SessionLoggedIn is (_ConnectedState & _NotAuthenticableState)

interface val _SessionState
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
     try
      let msg = _Message.startup(s.user, s.database)?
      s._connection().send(msg)
    else
      _Unreachable()
      None
    end

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
    _parse_response(s)

  fun _parse_response(s: Session ref) =>
    try
      while true do
        let message = _ResponseParser(s.readbuf)?
        match message
        | let msg: _AuthenticationMD5PasswordMessage =>
          s.state.on_authentication_md5_password(s, msg)
        | _AuthenticationOkMessage =>
          s.state.on_authentication_ok(s)
        | let err: _ErrorResponseMessage =>
          if (err.code == _ErrorCode.invalid_password())
            or (err.code == _ErrorCode.invalid_authentication_specification())
          then
            let reason = if err.code == _ErrorCode.invalid_password() then
              InvalidPassword
            else
              InvalidAuthenticationSpecification
            end

            s.state.on_authentication_failed(s, reason)
            shutdown(s)
          end
        | UnsupportedMessage =>
          // Unsupported message of a known type found. Continue on the way.
          None
        | None =>
          // No complete message was found in our received buffer, so we stop
          // parsing for now.
          return
        end
      end
    else
      // An unrecoverable error was encountered while parsing. Once that
      // happens, there's no way we are going to be able to figure out how
      // to get the responses back into an understandable state. The only
      // thing we can do is shut this session down.

      shutdown(s)
    end

  fun shutdown(s: Session ref) =>
    s.state = _SessionClosed
    s.readbuf.clear()
    s._connection().close()

trait _UnconnectedState is _NotAuthenticableState
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

trait _AuthenticableState is _ConnectedState
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

  fun on_authentication_failed(s: Session ref, r: AuthenticationFailureReason) =>
    _IllegalState()

  fun on_authentication_md5_password(s: Session ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    _IllegalState()
