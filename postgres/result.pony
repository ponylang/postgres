trait val Result
  fun query(): Query

class val ResultSet is Result
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
  let _query: Query

  new val create(query': Query) =>
    _query = query'

  fun query(): Query =>
    _query

class val RowModifying is Result
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
