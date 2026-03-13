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

// This example demonstrates COPY IN for bulk data loading. It creates a table,
// uses COPY FROM STDIN to load three rows of tab-delimited text data, verifies
// the data with a SELECT query, then drops the table.
//
// The COPY IN protocol uses a pull-based flow: the session calls pg_copy_ready
// after each send_copy_data, letting the client send the next chunk. When all
// data is sent, call finish_copy to complete the operation.
actor Client is (SessionStatusNotify & ResultReceiver & CopyInReceiver)
  let _session: Session
  let _out: OutStream
  var _phase: USize = 0
  var _rows_sent: USize = 0

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
      SimpleQuery("DROP TABLE IF EXISTS copy_in_example"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_copy_ready(session: Session) =>
    _rows_sent = _rows_sent + 1
    if _rows_sent <= 3 then
      // Send one row per callback. Tab-delimited, newline-terminated.
      let row: Array[U8] val = recover val
        ("row" + _rows_sent.string() + "\t" + (_rows_sent * 10).string()
          + "\n").array()
      end
      _out.print("  Sending row " + _rows_sent.string() + "...")
      session.send_copy_data(row)
    else
      _out.print("  All rows sent. Finishing COPY...")
      session.finish_copy()
    end

  be pg_copy_complete(session: Session, count: USize) =>
    _out.print("COPY complete: " + count.string() + " rows copied.")
    // Verify with a SELECT
    _out.print("Verifying with SELECT...")
    _session.execute(
      SimpleQuery("SELECT name, value FROM copy_in_example ORDER BY name"),
      this)

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _out.print("COPY failed: [" + e.severity + "] " + e.code + ": "
        + e.message)
    | let e: ClientQueryError =>
      _out.print("COPY failed: client error")
    end
    close()

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table dropped (or didn't exist). Create it.
      _out.print("Creating table...")
      _session.execute(
        SimpleQuery(
          """
          CREATE TABLE copy_in_example (
            name VARCHAR(50) NOT NULL,
            value INT NOT NULL
          )
          """),
        this)
    | 2 =>
      // Table created. Start COPY IN.
      _out.print("Table created. Starting COPY IN...")
      _session.copy_in(
        "COPY copy_in_example (name, value) FROM STDIN", this)
    | 3 =>
      // SELECT done. Print results and drop table.
      match result
      | let r: ResultSet =>
        _out.print("ResultSet (" + r.rows().size().string() + " rows):")
        for row in r.rows().values() do
          _out.write(" ")
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
      end
      _out.print("Dropping table...")
      _session.execute(
        SimpleQuery("DROP TABLE copy_in_example"), this)
    | 4 =>
      // Table dropped. Done.
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
