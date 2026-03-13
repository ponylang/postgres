"""
Querying binary data using `bytea` columns. Executes a SELECT that returns a
bytea value, matches on `Array[U8] val` in the result, and prints the decoded
bytes. Shows how the driver automatically decodes PostgreSQL's hex-format
bytea representation into raw byte arrays.
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
    _out.print("Sending bytea query....")
    // The hex string \x48656c6c6f represents the ASCII bytes for "Hello".
    let q = SimpleQuery("SELECT '\\x48656c6c6f'::bytea AS data")
    session.execute(q, this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      _out.print("ResultSet (" + r.rows().size().string() + " rows):")
      for row in r.rows().values() do
        for field in row.fields.values() do
          _out.write(field.name + "=")
          match field.value
          | let v: Array[U8] val =>
            _out.print(v.size().string() + " bytes")
            // Print each byte's decimal value
            for b in v.values() do
              _out.print("  byte: " + b.string())
            end
          | let v: String => _out.print(v)
          | let v: I16 => _out.print(v.string())
          | let v: I32 => _out.print(v.string())
          | let v: I64 => _out.print(v.string())
          | let v: F32 => _out.print(v.string())
          | let v: F64 => _out.print(v.string())
          | let v: Bool => _out.print(v.string())
          | None => _out.print("NULL")
          end
        end
      end
    | let r: RowModifying =>
      _out.print(r.command() + " " + r.impacted().string() + " rows")
    | let r: SimpleResult =>
      _out.print("Query executed.")
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
