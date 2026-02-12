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

// This example packs all the queries into a single actor using a _phase
// counter to sequence them. This keeps the example self-contained, but it's
// not how you'd structure a real application. Normally your application has
// its own state machine and issues queries as needed from different parts of
// your code rather than cramming everything into one client.
actor Client is (SessionStatusNotify & ResultReceiver)
  let _session: Session
  let _out: OutStream
  var _phase: USize = 0

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    _session = Session(
      ServerConnectInfo(auth, info.host, info.port),
      this,
      DatabaseConnectInfo(info.username, info.password, info.database))

  be close() =>
    _session.close()

  be pg_session_authenticated(session: Session) =>
    _out.print("Authenticated.")
    // Drop the table first in case a previous run was interrupted.
    _phase = 0
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS crud_example"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table dropped (or didn't exist). Create it.
      _out.print("Creating table...")
      _session.execute(
        SimpleQuery(
          """
          CREATE TABLE crud_example (
            name VARCHAR(50) NOT NULL,
            age INT NOT NULL
          )
          """),
        this)
    | 2 =>
      // Table created (SimpleResult). Insert first row with PreparedQuery.
      _out.print("Table created.")
      _out.print("Inserting rows...")
      _session.execute(
        PreparedQuery(
          "INSERT INTO crud_example (name, age) VALUES ($1, $2)",
          recover val [as (String | None): "Alice"; "30"] end),
        this)
    | 3 =>
      // First insert done. Show impacted count and insert second row.
      _print_row_modifying(result)
      _session.execute(
        PreparedQuery(
          "INSERT INTO crud_example (name, age) VALUES ($1, $2)",
          recover val [as (String | None): "Bob"; "25"] end),
        this)
    | 4 =>
      // Second insert done. Select all rows.
      _print_row_modifying(result)
      _out.print("Selecting rows...")
      _session.execute(
        PreparedQuery(
          "SELECT name, age FROM crud_example WHERE age >= $1 ORDER BY name",
          recover val [as (String | None): "0"] end),
        this)
    | 5 =>
      // Select done. Print results and delete all rows.
      match result
      | let r: ResultSet =>
        _out.print("ResultSet (" + r.rows().size().string() + " rows):")
        for row in r.rows().values() do
          _out.write(" ")
          for field in row.fields.values() do
            _out.write(" " + field.name + "=")
            match field.value
            | let v: String => _out.write(v)
            | let v: I16 => _out.write(v.string())
            | let v: I32 => _out.write(v.string())
            | let v: I64 => _out.write(v.string())
            | let v: F32 => _out.write(v.string())
            | let v: F64 => _out.write(v.string())
            | let v: Bool => _out.write(v.string())
            | None => _out.write("NULL")
            end
          end
          _out.print("")
        end
      end
      _out.print("Deleting rows...")
      _session.execute(
        SimpleQuery("DELETE FROM crud_example"), this)
    | 6 =>
      // Delete done. Show count and drop table.
      _print_row_modifying(result)
      _out.print("Dropping table...")
      _session.execute(
        SimpleQuery("DROP TABLE crud_example"), this)
    | 7 =>
      // Table dropped. Done.
      _out.print("Done.")
      close()
    end

  fun _print_row_modifying(result: Result) =>
    match result
    | let r: RowModifying =>
      _out.print(r.command() + " " + r.impacted().string() + " rows")
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _out.print("Query failed: [" + e.severity + "] " + e.code + ": "
        + e.message)
    | let e: ClientQueryError =>
      _out.print("Query failed: client error")
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
