type Query is (SimpleQuery | PreparedQuery | NamedPreparedQuery)
  """
  A query that can be executed via `Session.execute()`. SimpleQuery uses the
  simple query protocol (unparameterized); PreparedQuery uses the extended
  query protocol with an unnamed prepared statement (parameterized);
  NamedPreparedQuery executes a previously prepared named statement with
  parameters.
  """
