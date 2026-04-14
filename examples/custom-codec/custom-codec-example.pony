"""
Custom codec for PostgreSQL's `point` type (OID 600). Demonstrates how to
create a custom `FieldData` type, implement a binary `Codec` for it, register
the codec with `CodecRegistry.with_codec()`, and match on the custom type in
query results.
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

class val Point is (FieldData & Equatable[Point])
  """
  Custom decoded type for PostgreSQL `point` (OID 600).
  A point is two float8 values: (x, y).
  """
  let x: F64
  let y: F64

  new val create(x': F64, y': F64) =>
    x = x'
    y = y'

  fun eq(that: box->Point): Bool =>
    (x == that.x) and (y == that.y)

  fun string(): String iso^ =>
    recover iso
      let s = String
      s.append("(")
      s.append(x.string())
      s.append(",")
      s.append(y.string())
      s.append(")")
      s
    end

primitive PointBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `point` (OID 600).
  16 bytes: two IEEE 754 float8 values in big-endian order.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    // Encoding not needed for this example
    error

  fun decode(data: Array[U8] val): FieldData ? =>
    if data.size() != 16 then error end
    let x = ifdef bigendian then
      F64.from_bits(data.read_u64(0)?)
    else
      F64.from_bits(data.read_u64(0)?.bswap())
    end
    let y = ifdef bigendian then
      F64.from_bits(data.read_u64(8)?)
    else
      F64.from_bits(data.read_u64(8)?.bswap())
    end
    Point(x, y)

actor Client is (SessionStatusNotify & ResultReceiver)
  let _session: Session
  let _out: OutStream

  new create(auth: lori.TCPConnectAuth, info: ServerInfo, out: OutStream) =>
    _out = out
    // Register the custom codec for point (OID 600) and pass it to Session.
    // The fallback can't execute — OID 600 is not a built-in.
    let registry = try CodecRegistry.with_codec(600, PointBinaryCodec)?
    else CodecRegistry end
    _session = Session(
      ServerConnectInfo(auth, info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      this where registry = registry)

  be close() =>
    _session.close()

  be pg_session_authenticated(session: Session) =>
    _out.print("Authenticated.")
    _out.print("Querying point data...")
    // Use PreparedQuery to get binary format results
    let q = PreparedQuery(
      "SELECT '(1.5,2.5)'::point AS pt",
      recover val Array[FieldDataTypes] end)
    session.execute(q, this)

  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
  =>
    _out.print("Connection failed.")

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let r: ResultSet =>
      _out.print("ResultSet (" + r.rows().size().string() + " rows):")
      for row in r.rows().values() do
        for field in row.fields.values() do
          _out.write("  " + field.name + "=")
          match field.value
          | let p: Point =>
            _out.print("Point(" + p.x.string() + ", " + p.y.string() + ")")
          | let v: String => _out.print(v)
          | let v: I32 => _out.print(v.string())
          | None => _out.print("NULL")
          end
        end
      end
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
