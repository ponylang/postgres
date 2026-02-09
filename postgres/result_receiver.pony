// TODO SEAN: consider if each of these should take the session as well. If yes,
// it means that on success or failure, an actor that knows nothing about the
// session (ie no tag) could use it to execute additional commands after getting
// results. There are pros and cons to that.
interface tag ResultReceiver
  be pg_query_result(result: Result)
    """
    """

  be pg_query_failed(query: Query, failure: (ErrorResponseMessage | ClientQueryError))
    """
    """
