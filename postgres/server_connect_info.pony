use lori = "lori"

class val ServerConnectInfo
  """
  Connection parameters needed to reach the PostgreSQL server. Grouped because
  they are always used together — individually they have no meaning.
  """
  let auth: lori.TCPConnectAuth
  let host: String
  let service: String
  let ssl_mode: SSLMode

  new val create(auth': lori.TCPConnectAuth, host': String, service': String,
    ssl_mode': SSLMode = SSLDisabled)
  =>
    auth = auth'
    host = host'
    service = service'
    ssl_mode = ssl_mode'
