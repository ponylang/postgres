"""
Connection timeout using the `connection_timeout` parameter on
`ServerConnectInfo`. Connects to a configurable host and port with a 3-second
timeout. If the server is unreachable within the timeout, the session reports
`ConnectionFailedTimeout` via `pg_session_connection_failed`.
"""
use "cli"
use "collections"
use "constrained_types"
use lori = "lori"
// in your code this `use` statement would be:
// use "postgres"
use "../../postgres"

actor Main
  new create(env: Env) =>
    let server_info = ServerInfo(env.vars)
    let auth = lori.TCPConnectAuth(env.root)

    let client = Client(auth, server_info, env.out)

actor Client is SessionStatusNotify
  let _session: Session
  let _out: OutStream

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    match lori.MakeConnectionTimeout(3000)
    | let ct: lori.ConnectionTimeout =>
      _out.print("Connecting with 3-second timeout...")
      _session = Session(
        ServerConnectInfo(auth, info.host, info.port
          where connection_timeout' = ct),
        DatabaseConnectInfo(info.username, info.password, info.database),
        this)
    | let _: ValidationFailure =>
      _out.print("Failed to create connection timeout.")
      _session = Session(
        ServerConnectInfo(auth, info.host, info.port),
        DatabaseConnectInfo(info.username, info.password, info.database),
        this)
    end

  be pg_session_connected(session: Session) =>
    _out.print("Connected.")

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ConnectionFailedTimeout =>
      _out.print("Connection timed out.")
    | ConnectionFailedDNS =>
      _out.print("DNS resolution failed.")
    | ConnectionFailedTCP =>
      _out.print("TCP connection failed.")
    | SSLServerRefused =>
      _out.print("SSL refused by server.")
    | TLSAuthFailed =>
      _out.print("TLS authentication failed.")
    | TLSHandshakeFailed =>
      _out.print("TLS handshake failed.")
    end

  be pg_session_authenticated(session: Session) =>
    _out.print("Authenticated.")
    session.close()

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

class val ServerInfo
  let host: String
  let port: String
  let username: String
  let password: String
  let database: String

  new val create(vars: (Array[String] val | None)) =>
    let e = EnvVars(vars)
    host = try e("POSTGRES_HOST")? else "127.0.0.1" end
    port = try e("POSTGRES_PORT")? else "5432" end
    username = try e("POSTGRES_USERNAME")? else "postgres" end
    password = try e("POSTGRES_PASSWORD")? else "postgres" end
    database = try e("POSTGRES_DATABASE")? else "postgres" end
