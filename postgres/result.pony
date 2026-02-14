trait val Result
  """
  The result of a successfully executed query. Subtypes distinguish between
  queries that return rows (`ResultSet`), queries that modify rows
  (`RowModifying`), and queries that do neither (`SimpleResult`).
  """
  fun query(): Query

class val ResultSet is Result
  """
  A query result containing rows. Returned for SELECT and other row-returning
  statements. Provides access to the returned `Rows` and the command tag
  string (e.g., "SELECT").
  """
  let _query: Query
  let _rows: Rows
  let _command: String

  new val create(query': Query,
    rows': Rows,
    command': String)
  =>
    _query = query'
    _rows = rows'
    _command = command'

  fun query(): Query =>
    _query

  fun rows(): Rows =>
    _rows

  fun command(): String =>
    _command

class val SimpleResult is Result
  """
  A query result for statements that return no rows and report no row count.
  Returned for empty queries (the `EmptyQueryResponse` case).
  """
  let _query: Query

  new val create(query': Query) =>
    _query = query'

  fun query(): Query =>
    _query

class val RowModifying is Result
  """
  A query result for statements that modify rows (INSERT, UPDATE, DELETE).
  Provides the command tag string and the number of rows affected.
  """
  let _query: Query
  let _command: String
  let _impacted: USize

  new val create(query': Query,
    command': String,
    impacted': USize)
  =>
    _query = query'
    _command = command'
    _impacted = impacted'

  fun query(): Query =>
    _query

  fun command(): String =>
    _command

  fun impacted(): USize =>
    _impacted
