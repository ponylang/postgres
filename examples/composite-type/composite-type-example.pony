"""
Composite type support via `CodecRegistry.with_composite_type()`. Creates a
PostgreSQL composite type, discovers its OID from `pg_type`, registers it
with field descriptors, and queries it with `PreparedQuery` to get
`PgComposite` results. Uses two sessions to demonstrate the typical
two-phase pattern for dynamic OIDs: the first session discovers the OID,
the second uses a `CodecRegistry` built with that OID.

Shows positional access via `apply()`, named access via `field()`, and
sending a `PgComposite` as a query parameter.
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

actor Client is (SessionStatusNotify & ResultReceiver)
  let _auth: lori.TCPConnectAuth
  let _info: ServerInfo
  let _out: OutStream
  var _session: Session
  var _phase: USize = 0
  var _composite_oid: U32 = 0

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
      // Phase 1: set up the composite type and discover its OID.
      _out.print("Authenticated (phase 1: discover composite OID).")
      session.execute(
        SimpleQuery("DROP TYPE IF EXISTS address"), this)
    | 4 =>
      // Phase 2: query with composite-aware registry.
      _out.print("Authenticated (phase 2: query with registered composite).")
      _phase = 5

      // SELECT a composite literal
      session.execute(PreparedQuery(
        "SELECT ROW('123 Main St','Springfield',62704)::address AS addr",
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
      // Old type dropped. Create the composite type.
      _out.print("Creating composite type 'address'...")
      session.execute(SimpleQuery(
        "CREATE TYPE address AS (street text, city text, zip_code int4)"),
        this)
    | 2 =>
      // Type created. Query its OID from pg_type.
      _out.print("Querying composite OID from pg_type...")
      session.execute(
        SimpleQuery("SELECT oid FROM pg_type WHERE typname = 'address'"),
        this)
    | 3 =>
      // Got the OID. Parse it.
      var oid: U32 = 0
      match result
      | let rs: ResultSet =>
        try
          match rs.rows()(0)?.fields(0)?.value
          | let s: String => oid = s.u32()?
          end
        end
      end
      if oid == 0 then
        _out.print("Failed to discover composite OID.")
        close()
        return
      end
      _composite_oid = oid
      _out.print("Discovered composite OID: " + oid.string())

      // Register and query the array OID too if needed
      _out.print("Querying array OID from pg_type...")
      session.execute(
        SimpleQuery("SELECT typarray FROM pg_type WHERE oid = "
          + oid.string()),
        this)
    | 4 =>
      // Got the array OID. Build registry, close, reconnect.
      var array_oid: U32 = 0
      match result
      | let rs: ResultSet =>
        try
          match rs.rows()(0)?.fields(0)?.value
          | let s: String => array_oid = s.u32()?
          end
        end
      end

      let descriptors: Array[(String, U32)] val = recover val
        [as (String, U32): ("street", 25); ("city", 25); ("zip_code", 23)]
      end
      let registry = try
        let r = CodecRegistry
          .with_composite_type(_composite_oid, descriptors)?
        if array_oid > 0 then
          r.with_array_type(array_oid, _composite_oid)?
        else
          r
        end
      else
        _out.print("Failed to register composite type.")
        close()
        return
      end
      session.close()
      _session = Session(
        ServerConnectInfo(_auth, _info.host, _info.port),
        DatabaseConnectInfo(_info.username, _info.password, _info.database),
        this where registry = registry)
    | 6 =>
      // PreparedQuery result. The composite value arrives as PgComposite.
      match result
      | let rs: ResultSet =>
        _out.print("ResultSet (" + rs.rows().size().string() + " rows):")
        for row in rs.rows().values() do
          for field in row.fields.values() do
            match field.value
            | let c: PgComposite =>
              _out.print("  " + field.name + " (composite):")
              // Positional access
              try
                match c(0)?
                | let s: String => _out.print("    [0] street = " + s)
                | None => _out.print("    [0] street = NULL")
                end
              end
              // Named access
              try
                match c.field("city")?
                | let s: String => _out.print("    city = " + s)
                | None => _out.print("    city = NULL")
                end
              end
              try
                match c.field("zip_code")?
                | let v: I32 =>
                  _out.print("    zip_code = " + v.string())
                | None => _out.print("    zip_code = NULL")
                end
              end
              // String representation
              _out.print("    string() = " + c.string())
            | None => _out.print("  " + field.name + " = NULL")
            end
          end
        end
      end

      // Now send a PgComposite as a parameter
      _out.print("Sending PgComposite as query parameter...")
      let addr = PgComposite.from_fields(_composite_oid,
        recover val
          [as (String, U32, (FieldData | None)):
            ("street", 25, "42 Elm St")
            ("city", 25, "Portland")
            ("zip_code", 23, I32(97201))]
        end)
      session.execute(PreparedQuery(
        "SELECT $1::address AS roundtrip",
        recover val [as FieldDataTypes: addr] end), this)
    | 7 =>
      // Roundtrip result
      match result
      | let rs: ResultSet =>
        _out.print("Roundtrip result:")
        try
          match rs.rows()(0)?.fields(0)?.value
          | let c: PgComposite =>
            _out.print("  " + c.string())
          end
        end
      end
      // Clean up
      _out.print("Dropping composite type...")
      session.execute(SimpleQuery("DROP TYPE address"), this)
    | 8 =>
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
    | let _: SessionClosed =>
      return
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
