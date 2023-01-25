// in your code this `use` statement would be:
// use "postgres"
use "cli"
use "collections"
use lori = "lori"
use "../../postgres"

actor Main
  new create(env: Env) =>
    let server_info = ServerInfo(env.vars)
    let auth = lori.TCPConnectAuth(env.root)

    let client = Client(auth, server_info, env.out)

actor Client is (SessionStatusNotify & ResultReceiver)
  let _session: Session
  let _out: OutStream

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    _session = Session(
      auth,
      this,
      info.host,
      info.port,
      info.username,
      info.password,
      info.database)

  be pg_session_authenticated(session: Session) =>
    _out.print("Authenticated.")
    _out.print("Sending query....")
    let q = SimpleQuery("SELECT 525600::text")
    session.execute(q, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_query_result(result: Result) =>
    _out.print("Query result received.")

  be pg_query_failed(query: SimpleQuery, failure: QueryError) =>
    _out.print("Query failed.")

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
