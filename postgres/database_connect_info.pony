class val DatabaseConnectInfo
  """
  Database authentication parameters needed to log in to a PostgreSQL server.
  Grouped because they are always used together — individually they have no
  meaning.
  """
  let user: String
  let password: String
  let database: String

  new val create(user': String, password': String, database': String) =>
    user = user'
    password = password'
    database = database'
