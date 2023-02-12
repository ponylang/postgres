trait val Result
  fun query(): SimpleQuery

class val ResultSet is Result
  let _query: SimpleQuery
  let _rows: Rows
  let _command: String

  new val create(query': SimpleQuery,
    rows': Rows,
    command': String)
  =>
    _query = query'
    _rows = rows'
    _command = command'

  fun query(): SimpleQuery =>
    _query

  fun rows(): Rows =>
    _rows

  fun command(): String =>
    _command

class val SimpleResult is Result
  let _query: SimpleQuery

  new val create(query': SimpleQuery) =>
    _query = query'

  fun query(): SimpleQuery =>
    _query

class val RowModifying is Result
  let _query: SimpleQuery
  let _command: String
  let _impacted: USize

  new val create(query': SimpleQuery,
    command': String,
    impacted': USize)
  =>
    _query = query'
    _command = command'
    _impacted = impacted'

  fun query(): SimpleQuery =>
    _query

  fun command(): String =>
    _command

  fun impacted(): USize =>
    _impacted
