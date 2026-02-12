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

class val _AuthenticationSASLMessage
  """
  Message from the backend that indicates SASL authentication is required.
  Contains a list of supported SASL mechanism names (e.g., "SCRAM-SHA-256").
  """
  let mechanisms: Array[String] val

  new val create(mechanisms': Array[String] val) =>
    mechanisms = mechanisms'

class val _AuthenticationSASLContinueMessage
  """
  Message from the backend containing SASL challenge data (the server-first-
  message in a SCRAM exchange).
  """
  let data: Array[U8] val

  new val create(data': Array[U8] val) =>
    data = data'

class val _AuthenticationSASLFinalMessage
  """
  Message from the backend containing SASL completion data (the server-final-
  message in a SCRAM exchange). Contains the server's signature for mutual
  authentication verification.
  """
  let data: Array[U8] val

  new val create(data': Array[U8] val) =>
    data = data'

primitive _UnsupportedAuthenticationMessage
  """
  Message indicating the server requested an authentication method that this
  driver does not support (e.g., cleartext password, Kerberos, GSSAPI).
  """

class val _BackendKeyDataMessage
  """
  Message from the backend containing the process ID and secret key for this
  session. Sent once during startup, after AuthenticationOk and before
  ReadyForQuery. Required for query cancellation via CancelRequest.
  """
  let process_id: I32
  let secret_key: I32

  new val create(process_id': I32, secret_key': I32) =>
    process_id = process_id'
    secret_key = secret_key'

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

  fun val transaction_status(): TransactionStatus =>
    """
    Returns the transaction status reported by the server.
    """
    if _status == 'I' then TransactionIdle
    elseif _status == 'T' then TransactionInBlock
    else TransactionFailed
    end

class val _RowDescriptionMessage
  """
  Message from the backend that contains metadata like field names for any
  forthcoming DataRowMessages.
  """
  let columns: Array[(String, U32)] val

  new val create(columns': Array[(String, U32)] val) =>
    columns = columns'

primitive _ParseCompleteMessage
  """
  Message from the backend acknowledging that a Parse command has completed
  successfully.
  """

primitive _BindCompleteMessage
  """
  Message from the backend acknowledging that a Bind command has completed
  successfully.
  """

primitive _CloseCompleteMessage
  """
  Message from the backend acknowledging that a Close command has completed
  successfully.
  """

primitive _NoDataMessage
  """
  Message from the backend indicating that the described statement or portal
  will not return rows.
  """

class val _ParameterDescriptionMessage
  """
  Message from the backend describing the parameter types of a prepared
  statement. Sent in response to a Describe(Statement) command.
  """
  let param_oids: Array[U32] val

  new val create(param_oids': Array[U32] val) =>
    param_oids = param_oids'

class val _NotificationResponseMessage
  """
  Message from the backend delivering a LISTEN/NOTIFY notification.
  Contains the process ID of the notifying backend, the channel name,
  and the payload string.
  """
  let process_id: I32
  let channel: String
  let payload: String

  new val create(process_id': I32, channel': String, payload': String) =>
    process_id = process_id'
    channel = channel'
    payload = payload'

primitive _PortalSuspendedMessage
  """
  Message from the backend indicating that an Execute command has been
  suspended because the specified maximum row count was reached.
  """

