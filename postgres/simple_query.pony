class val SimpleQuery
  """
  An unparameterized query using PostgreSQL's simple query protocol. The query
  string may contain multiple semicolon-separated SQL statements; each
  statement produces its own result delivered to the `ResultReceiver`.
  """
  let string: String

  new val create(string': String) =>
    string = string'
