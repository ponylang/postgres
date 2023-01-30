trait val Result
  fun query(): SimpleQuery

class val ResultSet is Result
  let _query: SimpleQuery
  let _rows: Rows

  new val create(query': SimpleQuery, rows': Rows) =>
    _query = query'
    _rows = rows'

  fun query(): SimpleQuery =>
    _query

  fun rows(): Rows =>
    _rows

class val SimpleResult is Result
  let _query: SimpleQuery

  new val create(query': SimpleQuery) =>
    _query = query'

  fun query(): SimpleQuery =>
    _query

class val RowModifying is Result
  let _query: SimpleQuery
  let _impacted: USize

  new val create(query': SimpleQuery, impacted': USize) =>
    _query = query'
    _impacted = impacted'

  fun query(): SimpleQuery =>
    _query

  fun impacted(): USize =>
    _impacted
