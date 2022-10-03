use "buffered"
use lori = "lori"

primitive _Unopened
primitive _Connected
primitive _LoggedIn
primitive _Closed
type _SessionState is (_Unopened | _Connected | _LoggedIn | _Closed)

actor PgSession is lori.TCPClientActor
  let _auth: lori.TCPConnectAuth
  let _notify: PgSessionNotify
  let _host: String
  let _service: String
  let _user: String
  let _password: String
  let _database: String

  var _connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: _SessionState = _Unopened
  let _readbuf: Reader = Reader

  new create(
    auth: lori.TCPConnectAuth,
    notify: PgSessionNotify,
    host: String,
    service: String,
    user: String,
    password: String,
    database: String)
  =>
    _auth = auth
    _notify = consume notify
    _host = host
    _service = service
    _user = user
    _password = password
    _database = database

    _connection = lori.TCPConnection.client(auth, host, service, "", this)

  fun ref connection(): lori.TCPConnection =>
    _connection

  fun ref on_connected() =>
    _state = _Connected
    _notify.pg_session_connected(this)
    _send_startup_message()

  fun ref on_failure() =>
    _state = _Closed
    _notify.pg_session_connection_failed(this)

  fun ref on_received(data: Array[U8] iso) =>
    _readbuf.append(consume data)
    _parse_response()

  fun ref _shutdown() =>
    """
    Shutdown the session.
    """

    _readbuf.clear()
    _connection.close()
    _state = _Closed

  fun ref _send_startup_message() =>
    // TODO STA: assert that we have a state of _Connected
    try
      let msg = _Message.startup(_user, _database)?
      _connection.send(msg)
    else
      // TODO STA: this should never happen here
      None
    end

  fun ref _parse_response() =>
    try
      while true do
        let message = _ResponseParser(_readbuf)?
        match message
        | let msg: _AuthenticationMD5PasswordMessage =>
          let md5_password = _MD5Password(_user, _password, msg.salt)
          let reply = _Message.password(md5_password)
          _connection.send(reply)
        | _AuthenticationOkMessage =>
          _state = _LoggedIn
          _notify.pg_session_authenticated(this)
        | let err: _ErrorResponseMessage =>
          // TODO STA: need to handle invalid_authorization_specification here
          // as well.
          if err.code == _ErrorCode.invalid_password() then
            _notify.pg_session_authentication_failed(this)
            _shutdown()
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

      _shutdown()
    end
