class val ParameterStatus
  """
  A runtime parameter reported by the server. PostgreSQL sends these during
  connection startup and whenever a parameter's value changes (e.g., after
  a SET command). Common parameters include server_version, client_encoding,
  and standard_conforming_strings.
  """
  let name: String
  let value: String

  new val create(name': String, value': String) =>
    name = name'
    value = value'
