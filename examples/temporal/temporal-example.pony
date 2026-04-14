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
    _out.print("Querying temporal types...")
    // PreparedQuery returns typed temporal values via binary format.
    // Literal casts show all four temporal types without needing a table.
    let q = PreparedQuery(
      """
      SELECT '2024-06-15'::date AS d,
             '14:30:00.123456'::time AS t,
             '2024-06-15 14:30:00'::timestamp AS ts,
             '1 year 2 mons 3 days 04:05:06'::interval AS iv
      """,
      recover val Array[FieldDataTypes] end)
    session.execute(q, this)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _out.print("Connection failed.")

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      _out.print("ResultSet (" + r.rows().size().string() + " rows):")
      for row in r.rows().values() do
        for field in row.fields.values() do
          _out.write("  " + field.name + "=")
          match field.value
          | let v: PgDate => _out.print(v.string())
          | let v: PgTime => _out.print(v.string())
          | let v: PgTimestamp => _out.print(v.string())
          | let v: PgInterval => _out.print(v.string())
          | None => _out.print("NULL")
          else
            _out.print("(other type)")
          end
        end
      end
    end
    close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _out.print("Query failed.")
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
