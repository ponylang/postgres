use "buffered"
use lori = "lori"

actor PgSession is lori.TCPClientActor
  let notify: PgSessionNotify
  let host: String
  let service: String
  let user: String
  let password: String
  let database: String

  let readbuf: Reader = Reader
  var state: _PgSessionState = _PgSessionUnopened

  var _connection: lori.TCPConnection = lori.TCPConnection.none()

  new create(
    auth': lori.TCPConnectAuth,
    notify': PgSessionNotify,
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

    _connection = lori.TCPConnection.client(auth', host, service, "", this)

  fun ref connection(): lori.TCPConnection =>
    _connection

  fun ref on_connected() =>
    state.on_connected(this)

  fun ref on_failure() =>
    state.on_failure(this)

  fun ref on_received(data: Array[U8] iso) =>
    state.on_received(this, consume data)

// Possible session states
primitive _PgSessionUnopened is _ConnectableState
primitive _PgSessionClosed is (_NotConnectableState & _UnconnectedState)
primitive _PgSessionConnected is _AuthenticableState
primitive _PgSessionLoggedIn is (_ConnectedState & _NotAuthenticableState)

interface val _PgSessionState
  fun on_connected(s: PgSession ref)
    """
    Called when a connection is established with the server.
    """
  fun on_failure(s: PgSession ref)
    """
    Called if we fail to establish a connection with the server.
    """
  fun on_authentication_ok(s: PgSession ref)
    """
    Called when we successfully authenticate with the server.
    """
  fun on_authentication_failed(s: PgSession ref)
    """
    Called if we failed to successfully authenticate with the server.
    """
  fun on_authentication_md5_password(s: PgSession ref,
    msg: _AuthenticationMD5PasswordMessage)
    """
    Called if the server requests we autheticate using the Postgres MD5
    password scheme.
    """
  fun shutdown(s: PgSession ref)
    """
    Called when we are shutting down the session.
    """
  fun on_received(s: PgSession ref, data: Array[U8] iso)
    """
    Called when we receive data from the server.
    """

trait _ConnectableState is _UnconnectedState
  """
  An unopened session that can be connected to a server.
  """
  fun on_connected(s: PgSession ref) =>
    s.state = _PgSessionConnected
    s.notify.pg_session_connected(s)
    _send_startup_message(s)

  fun on_failure(s: PgSession ref) =>
    s.state = _PgSessionClosed
    s.notify.pg_session_connection_failed(s)

  fun _send_startup_message(s: PgSession ref) =>
     try
      let msg = _Message.startup(s.user, s.database)?
      s.connection().send(msg)
    else
      // TODO STA: this should never happen here
      None
    end

trait _NotConnectableState
  """
  A session that if it gets messages related to connect to a server, then
  something has gone wrong with the state machine.
  """
  fun on_connected(s: PgSession ref) =>
    // TODO STA: die out here if debug
    None

  fun on_failure(s: PgSession ref) =>
    // TODO STA: die out here if debug
    None

trait _ConnectedState is _NotConnectableState
  """
  A connected session. Connected sessions are not connectable as they have
  already been connected.
  """
  fun on_received(s: PgSession ref, data: Array[U8] iso) =>
    s.readbuf.append(consume data)
    _parse_response(s)

  fun _parse_response(s: PgSession ref) =>
    try
      while true do
        let message = _ResponseParser(s.readbuf)?
        match message
        | let msg: _AuthenticationMD5PasswordMessage =>
          s.state.on_authentication_md5_password(s, msg)
        | _AuthenticationOkMessage =>
          s.state.on_authentication_ok(s)
        | let err: _ErrorResponseMessage =>
          // TODO STA: need to handle invalid_authorization_specification here
          // as well.
          if err.code == _ErrorCode.invalid_password() then
            s.state.on_authentication_failed(s)
            shutdown(s)
          end
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

  fun shutdown(s: PgSession ref) =>
    s.state = _PgSessionClosed
    s.readbuf.clear()
    s.connection().close()

trait _UnconnectedState is _NotAuthenticableState
  """
  A session that isn't connected. Either because it was never opened or because
  it has been closed. Unconnected sessions are not eligible to be authenticated
  and receiving an authentication event while unconnected is an error.
  """
  fun on_received(s: PgSession ref, data: Array[U8] iso) =>
    // TODO STA: die out here if debug
    None

  fun shutdown(s: PgSession ref) =>
    // TODO STA: die out here if debug
    None

trait _AuthenticableState is _ConnectedState
  """
  A session that can be authenticated. All authenticatible sessions are
  connected sessions, but not all connected sessions are autheticable. Once a
  session has been authenticated, it's an error for another authetication event
  to occur.
  """
  fun on_authentication_ok(s: PgSession ref) =>
    s.state = _PgSessionLoggedIn
    s.notify.pg_session_authenticated(s)

  fun on_authentication_failed(s: PgSession ref) =>
    s.notify.pg_session_authentication_failed(s)
    shutdown(s)

  fun on_authentication_md5_password(s: PgSession ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
      let md5_password = _MD5Password(s.user, s.password, msg.salt)
      let reply = _Message.password(md5_password)
      s.connection().send(reply)

trait _NotAuthenticableState
  """
  A session that isn't eligible to be authenticated. Only connected sessions
  that haven't yet been authenticated are eligible to be authenticated.
  """
  fun on_authentication_ok(s: PgSession ref) =>
    // TODO STA: die out here if debug
    None

  fun on_authentication_failed(s: PgSession ref) =>
    // TODO STA: die out here if debug
    None

  fun on_authentication_md5_password(s: PgSession ref,
    msg: _AuthenticationMD5PasswordMessage)
  =>
    // TODO STA: die out here if debug
    None
