use lori = "lori"

class val ServerConnectInfo
  """
  Connection parameters needed to reach the PostgreSQL server. Grouped because
  they are always used together — individually they have no meaning.

  `auth_requirement` constrains which server-offered authentication methods
  the client will accept. Defaults to `AuthRequireSCRAM` (secure by default);
  set to `AllowAnyAuth` to connect to servers configured with MD5, cleartext,
  or trust authentication. See `AuthRequirement` for the rationale.

  An optional `connection_timeout` bounds the TCP connection phase. If the
  timeout fires before a TCP connection is established, `pg_session_connection_failed`
  is called with `ConnectionFailedTimeout`. Construct the timeout with
  `lori.MakeConnectionTimeout(milliseconds)`.
  """
  let auth: lori.TCPConnectAuth
  let host: String
  let service: String
  let ssl_mode: SSLMode
  let auth_requirement: AuthRequirement
  let connection_timeout: (lori.ConnectionTimeout | None)

  new val create(auth': lori.TCPConnectAuth, host': String, service': String,
    ssl_mode': SSLMode = SSLDisabled,
    auth_requirement': AuthRequirement = AuthRequireSCRAM,
    connection_timeout': (lori.ConnectionTimeout | None) = None)
  =>
    auth = auth'
    host = host'
    service = service'
    ssl_mode = ssl_mode'
    auth_requirement = auth_requirement'
    connection_timeout = connection_timeout'
