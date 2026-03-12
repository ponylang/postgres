class val NamedPreparedQuery
  """
  Executes a previously prepared server-side statement by name. The statement
  must have been created via `Session.prepare()` before executing this query.

  Parameters are referenced as $1, $2, ... in the original SQL string that was
  passed to `prepare()`. Typed values are sent in binary format with explicit
  OIDs; `String` values are sent in text format with server inference. Use
  `None` for NULL.
  """
  let name: String
  let params: Array[FieldDataTypes] val

  new val create(name': String, params': Array[FieldDataTypes] val) =>
    name = name'
    params = params'
