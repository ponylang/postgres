class val NamedPreparedQuery
  """
  Executes a previously prepared server-side statement by name. The statement
  must have been created via `Session.prepare()` before executing this query.

  Parameters are referenced as $1, $2, ... in the original SQL string that was
  passed to `prepare()`. Values are sent in text format; use None for NULL.
  """
  let name: String
  let params: Array[(String | None)] val

  new val create(name': String, params': Array[(String | None)] val) =>
    name = name'
    params = params'
