class val _AuthenticationMD5PasswordMessage
  """
  Message from the backend that indicates that MD5 authentication is being
  requested by the server. Contains a salt needed to construct the reply.
  """
  let salt: String

  new val create(salt': String) =>
    salt = salt'

primitive _AuthenticationOkMessage
  """
  Message from the backend that indicates that a session has been successfully
  authenticated.
  """

class val _CommandCompleteMessage
  """
  Messagr from the backend that indicates that a command has finished running.
  The message contains information about final details of the command.
  """
  let id: String
  let value: USize

  new val create(id': String, value': USize) =>
    id = id'
    value = value'

class val _DataRowMessage
  """
  Message from the backend that represents a row of data from something like a
  SELECT.
  """
  let columns: Array[(String|None)] val

  new val create(columns': Array[(String|None)] val) =>
    columns = columns'

primitive _EmptyQueryResponseMessage
  """
  Message from the backend that acknowledges the receipt of an empty query.
  """

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

  fun val failed_transaction(): Bool =>
    """
    Returns true if the backend is in a failed transaction block. Queries will
    be rejected until the transaction has ended.
    """
    _status == 'E'

class val _RowDescriptionMessage
  """
  Message from the backend that contains metadata like field names for any
  forthcoming DataRowMessages.
  """
  let columns: Array[(String, U32)] val

  new val create(columns': Array[(String, U32)] val) =>
    columns = columns'

