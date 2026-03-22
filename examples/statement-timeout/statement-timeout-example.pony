"""
Statement timeout using the `statement_timeout` parameter on
`session.execute()`. Executes a long-running query (`SELECT pg_sleep(10)`)
with a 2-second timeout. When the timeout fires, the driver sends a
CancelRequest and the query fails with SQLSTATE 57014 (query_canceled).
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

actor Client is (SessionStatusNotify & ResultReceiver)
  let _session: Session
  let _out: OutStream

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    _session = Session(
      ServerConnectInfo(auth, info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this)

  be close() =>
    _session.close()

  be pg_session_authenticated(session: Session) =>
    _out.print("Authenticated.")

    match lori.MakeTimerDuration(2000)
    | let d: lori.TimerDuration =>
      _out.print("Sending long-running query with 2-second timeout....")
      let q = SimpleQuery("SELECT pg_sleep(10)")
      session.execute(q, this where statement_timeout = d)
    | let vf: ValidationFailure =>
      _out.print("Failed to create timer duration.")
      close()
    end

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_query_result(session: Session, result: Result) =>
    _out.print("Query completed (was not cancelled).")
    close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let err: ErrorResponseMessage =>
      if err.code == "57014" then
        _out.print("Query timed out (SQLSTATE 57014).")
      else
        _out.print("Query failed with SQLSTATE " + err.code + ".")
      end
    | let ce: ClientQueryError =>
      _out.print("Query failed with client error.")
    end
    close()

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
