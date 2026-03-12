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

// This example demonstrates query pipelining for reduced round-trip latency.
// It creates a table with 3 rows, pipelines 3 SELECTs with different WHERE
// clauses in a single call, prints the indexed results, then drops the table.
//
// Pipelining sends all queries to the server in one TCP write and processes
// responses as they arrive. Each query has its own error isolation boundary —
// if one fails, the others continue executing.
actor Client is
  (SessionStatusNotify & ResultReceiver & PipelineReceiver)
  let _session: Session
  let _out: OutStream
  var _phase: USize = 0

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
    _phase = 0
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS pipeline_example"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1
    match _phase
    | 1 =>
      _out.print("Creating table...")
      _session.execute(
        SimpleQuery(
          """
          CREATE TABLE pipeline_example (
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL
          )
          """),
        this)
    | 2 =>
      _out.print("Inserting rows...")
      _session.execute(
        SimpleQuery(
          """
          INSERT INTO pipeline_example VALUES
            (1, 'alpha'), (2, 'bravo'), (3, 'charlie')
          """),
        this)
    | 3 =>
      _out.print("Pipelining 3 SELECTs...")
      let queries = recover val
        [as (PreparedQuery | NamedPreparedQuery):
          PreparedQuery(
            "SELECT id, name FROM pipeline_example WHERE id = $1",
            recover val [as (String | None): "1"] end)
          PreparedQuery(
            "SELECT id, name FROM pipeline_example WHERE id = $1",
            recover val [as (String | None): "2"] end)
          PreparedQuery(
            "SELECT id, name FROM pipeline_example WHERE id = $1",
            recover val [as (String | None): "3"] end)
        ]
      end
      _session.pipeline(queries, this)
    | 5 =>
      _out.print("Done.")
      close()
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match \exhaustive\ failure
    | let e: ErrorResponseMessage =>
      _out.print("Query failed: [" + e.severity + "] " + e.code + ": "
        + e.message)
    | let e: ClientQueryError =>
      _out.print("Query failed: client error")
    end
    close()

  be pg_pipeline_result(session: Session, index: USize, result: Result) =>
    _out.print("  Pipeline query " + index.string() + ":")
    match result
    | let rs: ResultSet =>
      for row in rs.rows().values() do
        _out.write("   ")
        for field in row.fields.values() do
          _out.write(" " + field.name + "=")
          match field.value
          | let v: String => _out.write(v)
          | let v: I32 => _out.write(v.string())
          | None => _out.write("NULL")
          end
        end
        _out.print("")
      end
    | let rm: RowModifying =>
      _out.print("    " + rm.command() + ": " + rm.impacted().string()
        + " rows")
    | let _: SimpleResult =>
      _out.print("    (empty)")
    end

  be pg_pipeline_failed(session: Session, index: USize,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match \exhaustive\ failure
    | let e: ErrorResponseMessage =>
      _out.print("  Pipeline query " + index.string() + " failed: ["
        + e.severity + "] " + e.code + ": " + e.message)
    | let e: ClientQueryError =>
      _out.print("  Pipeline query " + index.string()
        + " failed: client error")
    end

  be pg_pipeline_complete(session: Session) =>
    _out.print("Pipeline complete.")
    _out.print("Dropping table...")
    _phase = _phase + 1
    _session.execute(
      SimpleQuery("DROP TABLE pipeline_example"), this)

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
