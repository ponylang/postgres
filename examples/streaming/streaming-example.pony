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

// This example demonstrates row streaming for pull-based paged result
// consumption. It creates a table with 7 rows, streams them with a window
// size of 3 (producing batches of 3, 3, and 1), then drops the table.
//
// The streaming protocol uses a pull-based flow: the session delivers a
// batch via pg_stream_batch, then the client calls fetch_more() to request
// the next batch. When no more rows remain, pg_stream_complete fires.
actor Client is
  (SessionStatusNotify & ResultReceiver & StreamingResultReceiver)
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
      SimpleQuery("DROP TABLE IF EXISTS streaming_example"), this)

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
          CREATE TABLE streaming_example (
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
          INSERT INTO streaming_example VALUES
            (1, 'alpha'), (2, 'bravo'), (3, 'charlie'),
            (4, 'delta'), (5, 'echo'), (6, 'foxtrot'), (7, 'golf')
          """),
        this)
    | 3 =>
      _out.print("Starting stream with window_size=3...")
      _session.stream(
        PreparedQuery(
          "SELECT id, name FROM streaming_example ORDER BY id",
          recover val Array[(String | None)] end),
        3, this)
    | 5 =>
      _out.print("Done.")
      close()
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

  be pg_stream_batch(session: Session, rows: Rows) =>
    _out.print("  Batch (" + rows.size().string() + " rows):")
    for row in rows.values() do
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
    session.fetch_more()

  be pg_stream_complete(session: Session) =>
    _out.print("Stream complete.")
    _out.print("Dropping table...")
    _phase = _phase + 1
    _session.execute(
      SimpleQuery("DROP TABLE streaming_example"), this)

  be pg_stream_failed(session: Session,
    query: (PreparedQuery | NamedPreparedQuery),
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _out.print("Stream failed: [" + e.severity + "] " + e.code + ": "
        + e.message)
    | let e: ClientQueryError =>
      _out.print("Stream failed: client error")
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
