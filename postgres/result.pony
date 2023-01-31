trait val Result
  fun query(): SimpleQuery

class val ResultSet is Result
  let _query: SimpleQuery
  let _rows: Rows
  let _command: CommandCompleteMessage

  new val create(query': SimpleQuery,
    rows': Rows,
    command': CommandCompleteMessage)
  =>
    _query = query'
    _rows = rows'
    _command = command'

  fun query(): SimpleQuery =>
    _query

  fun rows(): Rows =>
    _rows

  fun command(): CommandCompleteMessage =>
    _command

class val SimpleResult is Result
  let _query: SimpleQuery

  new val create(query': SimpleQuery) =>
    _query = query'

  fun query(): SimpleQuery =>
    _query

class val RowModifying is Result
  let _query: SimpleQuery
  let _command: CommandCompleteMessage

  new val create(query': SimpleQuery, command': CommandCompleteMessage) =>
    _query = query'
    _command = command'

  fun query(): SimpleQuery =>
    _query

  fun command(): CommandCompleteMessage =>
    _command
