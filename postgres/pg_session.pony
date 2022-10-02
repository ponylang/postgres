use "buffered"
use lori = "lori"

actor PgSession is lori.TCPClientActor
  let _auth: lori.TCPConnectAuth
  let _notify: PgSessionNotify iso
  let _host: String
  let _service: String
  let _user: String
  let _password: String
  let _database: String

  var _connection: lori.TCPConnection = lori.TCPConnection.none()

  let _readbuf: Reader = Reader

  new create(
    auth: lori.TCPConnectAuth,
    notify: PgSessionNotify iso,
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
    _notify.on_connected()
    _send_startup_message()

  fun ref on_failure() =>
    _notify.on_connection_failed()

  fun ref on_received(data: Array[U8] iso) =>
    _readbuf.append(consume data)
    _parse_response()

  fun ref _send_startup_message() =>
    try
      let msg = _Message.startup(_user, _database)?
      _connection.send(msg)
    else
      // TODO STA: this should never happen here
      None
    end

  fun ref _parse_response() =>
    while true do
      let message = _ResponseParser(_readbuf)
      match message
      | let msg: _AuthenticationMD5PasswordMessage =>
        let md5_password = _MD5Password(_user, _password, msg.salt)
        let reply = _Message.password(md5_password)
        _connection.send(reply)
      | _AuthenticationOkMessage =>
        _notify.on_authenticated()
      | let err: _ErrorResponseMessage =>
        // TODO STA: need to handle invalid_authorization_specification here
        // as well.
        if err.code == _ErrorCode.invalid_password() then
          _notify.on_authentication_failed()
        end
      | None =>
        // No complete message was found in our received buffer, so we stop
        // parsing for now.
        return
      end
    end
