class val PreparedQuery
  """
  A parameterized query using PostgreSQL's extended query protocol.
  Parameters are referenced as $1, $2, ... in the query string.

  Typed parameter values are sent in binary format with explicit OIDs
  (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`).
  `String` values are sent in text format with OID 0 (server infers the
  type), preserving backward compatibility — the server parses the text
  representation for whatever type the column expects. Use `None` for NULL.

  The query string must contain a single SQL statement. Multi-statement
  queries are not supported by the extended query protocol; use SimpleQuery
  for multi-statement execution.
  """
  let string: String
  let params: Array[FieldDataTypes] val

  new val create(string': String, params': Array[FieldDataTypes] val) =>
    string = string'
    params = params'
