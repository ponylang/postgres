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
    _check_for_complete_message()

  fun ref _send_startup_message() =>
    try
      let msg = _Message.startup(_user, _database)?
      _connection.send(msg)
    else
      // TODO STA: this should never happen here
      None
    end

  fun ref _check_for_complete_message() =>
    // TODO STA: we can make progress into examining a message and then realize
    // that there isn't enough. We don't want to redo all the original examining
    // every single time. We should take an iterative approach storing the state
    // we have found so far

    // The minimum size for any complete message is 9. If we have less than
    // 9 received bytes buffered than there is no point to continuing as we
    // definitely don't have a full message.
    if _readbuf.size() < 9 then
      return
    end

    // TODO STA: this try block is way to long and can hide a ton of errors
    // during development
    try
      let message_type = _readbuf.peek_u8(0)?
      // payload size includes the 4 bytes for the descriptive header on the
      // payload.
      let payload_size = _readbuf.peek_u32_be(1)?.usize() - 4
      let message_size = payload_size + 4 + 1

      // The message will be `message_size` in length. If we have less than
      // that then there's no point in continuing.
      if _readbuf.size() < message_size then
        return
      end

      match message_type
      | _MessageType.authentication_request() =>
        let auth_type = _readbuf.peek_i32_be(5)?

        if auth_type == _AuthenticationRequestType.ok() then
          // discard the message and type header
          _readbuf.skip(message_size)?
          // notify that we are authenticated
          _notify.on_authenticated()
        elseif auth_type == _AuthenticationRequestType.md5_password() then
          // Slide past the header...
          _readbuf.skip(5)?
          // and only get the payload
          let payload = _readbuf.block(payload_size)?
          let password = _MD5PasswordFromMessagePayload(consume payload, _user, _password)?
          let message = _Message.password(password)
          _connection.send(message)
        else
          // TODO STA: unsupported auth type
          None
        end
      | _MessageType.error_response() =>
        // Slide past the header...
        _readbuf.skip(5)?
        // and only get the payload
        let payload = _readbuf.block(payload_size)?
        let err = _ErrorResponseParser(consume payload)?
        if err.code == _ErrorCode.invalid_password() then
          _notify.on_authentication_failed()
        end
      else
        // TODO STA: unknown message type
        None
      end
    end

