interface tag ResultReceiver
  be pg_query_result(result: Result)
    """
    """

  be pg_query_failed(query: SimpleQuery, failure: QueryError)
    """
    """
