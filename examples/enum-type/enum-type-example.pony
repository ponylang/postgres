"""
Enum type support via `CodecRegistry.with_enum_type()`. Creates a PostgreSQL
enum type, discovers its OID from `pg_type`, registers the OID with
`with_enum_type`, and queries the enum with `PreparedQuery` to get `String`
results in binary format. Without registration, `PreparedQuery` would return
`RawBytes` for unknown binary OIDs.

This example uses two sessions: the first discovers the enum OID (which is
dynamically assigned by PostgreSQL), and the second uses a `CodecRegistry`
built with that OID. This two-phase pattern is typical for enum types since
their OIDs aren't known at compile time.
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
    Client(auth, server_info, env.out)

// Two-phase example: first session discovers the enum OID, second session
// uses a CodecRegistry with that OID registered. Packed into one actor with
// a _phase counter for self-containment — a real application would typically
// store the OID in configuration or discover it once at startup.
actor Client is (SessionStatusNotify & ResultReceiver)
  let _auth: lori.TCPConnectAuth
  let _info: ServerInfo
  let _out: OutStream
  var _session: Session
  var _phase: USize = 0

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _auth = auth
    _info = info
    _out = out
    _session = Session(
      ServerConnectInfo(_auth, _info.host, _info.port),
      DatabaseConnectInfo(_info.username, _info.password, _info.database),
      this)

  be close() =>
    _session.close()

  be pg_session_authenticated(session: Session) =>
    match _phase
    | 0 =>
      // Phase 1: set up the enum type and discover its OID.
      _out.print("Authenticated (phase 1: discover enum OID).")
      session.execute(
        SimpleQuery("DROP TYPE IF EXISTS mood"), this)
    | 3 =>
      // Phase 2: query with enum-aware registry.
      _out.print("Authenticated (phase 2: query with registered enum OID).")
      _phase = 4
      session.execute(PreparedQuery(
        "SELECT 'happy'::mood AS mood_col",
        recover val Array[FieldDataTypes] end), this)
    end

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _out.print("Connection failed.")

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1

    match _phase
    | 1 =>
      // Old type dropped. Create the enum.
      _out.print("Creating enum type 'mood'...")
      session.execute(
        SimpleQuery("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')"),
        this)
    | 2 =>
      // Enum created. Query its OID from pg_type.
      _out.print("Querying enum OID from pg_type...")
      session.execute(
        SimpleQuery("SELECT oid FROM pg_type WHERE typname = 'mood'"), this)
    | 3 =>
      // Got the OID. Parse it, close this session, and start phase 2.
      var enum_oid: U32 = 0
      match result
      | let rs: ResultSet =>
        try
          match rs.rows()(0)?.fields(0)?.value
          | let s: String => enum_oid = s.u32()?
          end
        end
      end
      if enum_oid == 0 then
        _out.print("Failed to discover enum OID.")
        close()
        return
      end
      _out.print("Discovered enum OID: " + enum_oid.string())

      // Build a registry with the enum OID and reconnect.
      let registry = try CodecRegistry.with_enum_type(enum_oid)?
      else
        _out.print("Failed to register enum OID.")
        close()
        return
      end
      session.close()
      _session = Session(
        ServerConnectInfo(_auth, _info.host, _info.port),
        DatabaseConnectInfo(_info.username, _info.password, _info.database),
        this where registry = registry)
    | 5 =>
      // PreparedQuery result. The enum value arrives as String.
      match result
      | let rs: ResultSet =>
        _out.print("ResultSet (" + rs.rows().size().string() + " rows):")
        for row in rs.rows().values() do
          for field in row.fields.values() do
            _out.write("  " + field.name + "=")
            match field.value
            | let s: String => _out.print(s)
            | None => _out.print("NULL")
            end
          end
        end
      end
      // Clean up: drop the enum type.
      _out.print("Dropping enum type...")
      session.execute(SimpleQuery("DROP TYPE mood"), this)
    | 6 =>
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
