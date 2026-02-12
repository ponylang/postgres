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

// This example demonstrates transaction status tracking. It runs a BEGIN /
// INSERT / COMMIT sequence and prints the transaction status reported by
// pg_transaction_status at each step.
actor Client is (SessionStatusNotify & ResultReceiver)
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
    // Drop the table first in case a previous run was interrupted.
    _phase = 0
    session.execute(
      SimpleQuery("DROP TABLE IF EXISTS txn_example"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _out.print("Failed to authenticate.")

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    match status
    | TransactionIdle => _out.print("Transaction status: idle")
    | TransactionInBlock => _out.print("Transaction status: in transaction")
    | TransactionFailed => _out.print("Transaction status: failed")
    end

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Table dropped. Create it.
      _out.print("Creating table...")
      _session.execute(
        SimpleQuery(
          "CREATE TABLE txn_example (name VARCHAR(50) NOT NULL)"),
        this)
    | 2 =>
      // Table created. Start a transaction.
      _out.print("Table created. Starting transaction...")
      _session.execute(SimpleQuery("BEGIN"), this)
    | 3 =>
      // BEGIN done. Insert a row inside the transaction.
      _out.print("Inserting row inside transaction...")
      _session.execute(
        SimpleQuery("INSERT INTO txn_example (name) VALUES ('Alice')"), this)
    | 4 =>
      // Insert done. Commit.
      _out.print("Committing transaction...")
      _session.execute(SimpleQuery("COMMIT"), this)
    | 5 =>
      // Commit done. Verify the row is there.
      _out.print("Verifying...")
      _session.execute(
        SimpleQuery("SELECT name FROM txn_example"), this)
    | 6 =>
      // Select done. Print and clean up.
      match result
      | let r: ResultSet =>
        _out.print("Rows after commit: " + r.rows().size().string())
      end
      _session.execute(
        SimpleQuery("DROP TABLE txn_example"), this)
    | 7 =>
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
