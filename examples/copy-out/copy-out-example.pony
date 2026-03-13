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

// This example demonstrates COPY OUT for bulk data export. It creates a table,
// inserts three rows, uses COPY TO STDOUT to export them, prints the received
// data, verifies the row count, then drops the table.
//
// The COPY OUT protocol is server-driven: after the session sends the COPY
// query, the server pushes data via pg_copy_data callbacks. When all data is
// sent, pg_copy_complete fires with the row count.
actor Client is (SessionStatusNotify & ResultReceiver & CopyOutReceiver)
  let _session: Session
  let _out: OutStream
  var _phase: USize = 0
  var _copy_data: Array[U8] iso = recover iso Array[U8] end

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
      SimpleQuery("DROP TABLE IF EXISTS copy_out_example"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_copy_data(session: Session, data: Array[U8] val) =>
    _copy_data.append(data)

  be pg_copy_complete(session: Session, count: USize) =>
    let received: String val = String.from_iso_array(
      _copy_data = recover iso Array[U8] end)
    _out.print("COPY complete: " + count.string() + " rows exported.")
    _out.print("Received data:")
    _out.print(received)
    // Drop the table
    _out.print("Dropping table...")
    _session.execute(
      SimpleQuery("DROP TABLE copy_out_example"), this)

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
          CREATE TABLE copy_out_example (
            name VARCHAR(50) NOT NULL,
            value INT NOT NULL
          )
          """),
        this)
    | 2 =>
      // Table created. Insert rows.
      _out.print("Inserting rows...")
      _session.execute(
        SimpleQuery(
          "INSERT INTO copy_out_example VALUES " +
          "('alice', 10), ('bob', 20), ('charlie', 30)"),
        this)
    | 3 =>
      // Rows inserted. Start COPY OUT.
      _out.print("Starting COPY OUT...")
      _session.copy_out(
        "COPY copy_out_example TO STDOUT", this)
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
