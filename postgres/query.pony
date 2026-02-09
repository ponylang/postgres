type Query is (SimpleQuery | PreparedQuery)
  """
  A query that can be executed via `Session.execute()`. SimpleQuery uses the
  simple query protocol (unparameterized); PreparedQuery uses the extended
  query protocol (parameterized).
  """
