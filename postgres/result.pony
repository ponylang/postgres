class val Result
  let _query: SimpleQuery
  let _rows: Rows

  new val create(query': SimpleQuery, rows': Rows) =>
    _query = query'
    _rows = rows'

  fun query(): SimpleQuery =>
    _query

  fun rows(): Rows =>
    _rows
