"""
Multi-host connection failover as a user-space pattern. Creates sessions to
multiple PostgreSQL hosts in parallel and uses the first one to authenticate.
Remaining sessions are closed. Demonstrates how to build failover on top of
the driver's single-host `Session` without any driver modifications.

Two of the three hosts are intentionally unreachable (127.0.0.2 on unused
ports), so the session targeting the real server wins the race.
"""
use "cli"
use "collections"
use lori = "lori"
// in your code this `use` statement would be:
// use "postgres"
use "../../postgres"

actor Main
  new create(env: Env) =>
    let server_info = ServerInfo(env.vars)
    let auth = lori.TCPConnectAuth(env.root)

    Failover(auth, server_info, env.out)

actor Failover is (SessionStatusNotify & ResultReceiver)
  let _hosts: Array[(Session tag, String val)]
  let _out: OutStream
  let _total: USize
  var _failures: USize = 0
  var _winner: (Session | None) = None

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    let db = DatabaseConnectInfo(info.username, info.password, info.database)

    // Two intentionally bad hosts plus the real one from env vars.
    // 127.0.0.2 avoids the WSL2 mirrored networking hang on 127.0.0.1.
    let targets: Array[(String, String)] val = [
      ("127.0.0.2", "19999")
      ("127.0.0.2", "19998")
      (info.host, info.port)
    ]
    _total = targets.size()

    _hosts = Array[(Session tag, String val)](targets.size())
    for (host, port) in targets.values() do
      let label: String val = host + ":" + port
      let session = Session(ServerConnectInfo(auth, host, port), db, this)
      _hosts.push((session, label))
      _out.print("Trying " + label + "...")
    end

  fun _label_for(session: Session): String val =>
    for (s, label) in _hosts.values() do
      if s is session then return label end
    end
    "unknown"

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _out.print(_label_for(session) + " — connection failed.")
    _on_failure()

  be pg_session_authentication_failed(session: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print(_label_for(session) + " — authentication failed.")
    _on_failure()

  fun ref _on_failure() =>
    _failures = _failures + 1
    if (_failures == _total) and (_winner is None) then
      _out.print("All hosts failed.")
    end

  be pg_session_authenticated(session: Session) =>
    match _winner
    | None =>
      _out.print(_label_for(session) + " — authenticated (winner).")
      _winner = session
      // Close the other sessions. close() is a no-op on sessions still
      // connecting (_SessionUnopened) — late arrivals are handled below.
      for (s, _) in _hosts.values() do
        if s isnt session then s.close() end
      end
      // Prove the connection works.
      session.execute(SimpleQuery("SELECT 1::text"), this)
    | let _: Session =>
      // Late arrival — a session that was still connecting when we picked
      // a winner has now authenticated. Close it.
      _out.print(_label_for(session) + " — late arrival, closing.")
      session.close()
    end

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      _out.print("Query succeeded on winner ("
        + r.rows().size().string() + " row).")
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _out.print("Query failed on winner.")
    session.close()

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
