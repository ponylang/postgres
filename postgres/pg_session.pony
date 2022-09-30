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

  fun ref on_failure() =>
    _notify.on_connection_failed()
