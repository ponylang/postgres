class val Result
  let _query: SimpleQuery

  new val create(query': SimpleQuery) =>
    _query = query'

  fun query(): SimpleQuery =>
    _query
