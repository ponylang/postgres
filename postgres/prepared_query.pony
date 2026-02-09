class val PreparedQuery
  """
  A parameterized query using PostgreSQL's extended query protocol.
  Parameters are referenced as $1, $2, ... in the query string.
  Values are sent in text format; use None for NULL.

  The query string must contain a single SQL statement. Multi-statement
  queries are not supported by the extended query protocol; use SimpleQuery
  for multi-statement execution.
  """
  let string: String
  let params: Array[(String | None)] val

  new val create(string': String, params': Array[(String | None)] val) =>
    string = string'
    params = params'
