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
  var _step: USize = 0

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

    // Step 1: SELECT a literal array via PreparedQuery (binary decode).
    _out.print("Querying int4[] via PreparedQuery...")
    session.execute(PreparedQuery(
      "SELECT ARRAY[10, 20, 30]::int4[] AS nums",
      recover val Array[FieldDataTypes] end), this)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _out.print("Connection failed.")

  be pg_query_result(session: Session, result: Result) =>
    _step = _step + 1
    match result
    | let r: ResultSet =>
      for row in r.rows().values() do
        for field in row.fields.values() do
          _out.write("  " + field.name + "=")
          match field.value
          | let a: PgArray =>
            _out.print(a.string())
            for elem in a.elements.values() do
              match elem
              | let v: I32 => _out.print("    element: " + v.string())
              | let v: String => _out.print("    element: " + v)
              | None => _out.print("    element: NULL")
              else
                _out.print("    element: (other type)")
              end
            end
          | None => _out.print("NULL")
          else
            _out.print("(other type)")
          end
        end
      end
    end

    if _step == 1 then
      // Step 2: Send a PgArray as a query parameter (encode + decode roundtrip).
      _out.print("Sending int4[] parameter via PreparedQuery...")
      let arr = PgArray(23,
        recover val [as (FieldData | None): I32(100); None; I32(300)] end)
      session.execute(PreparedQuery("SELECT $1::int4[] AS roundtrip",
        recover val [as FieldDataTypes: arr] end), this)
    else
      close()
    end

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
