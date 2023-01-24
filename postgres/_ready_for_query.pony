class val _ReadyForQueryMessage
  """
  Message from the backend that indicates that it is ready for a new query
  cycle. Also sent as the last step in the session start up process.
  """
  let _status: U8

  new val create(status: U8) =>
    _status = status

  fun val idle(): Bool =>
    """
    Returns true if the backend status is "idle"
    """
    _status == 'I'

  fun val in_transaction_block(): Bool =>
    """
    Returns true if the backend is executing a transaction
    """
    _status == 'T'

  fun val idle(): Bool =>
    """
    Returns true if the backend is in a failed transaction block. Queries will
    be rejected until the transaction has ended.
    """
    _status == 'E'
