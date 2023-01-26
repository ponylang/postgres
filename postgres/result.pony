class val Result
  let _query: SimpleQuery
  let _rows: Array[Array[(String|None)] val] val

  new val create(query': SimpleQuery,
    rows': Array[Array[(String|None)] val] val)
  =>
    _query = query'
    _rows = rows'

  fun query(): SimpleQuery =>
    _query

  fun rows(): Array[Array[(String|None)] val] val =>
    _rows
