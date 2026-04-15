primitive ConnectionFailedDNS
  """
  Name resolution failed — the server hostname could not be resolved.
  """

primitive ConnectionFailedTCP
  """
  TCP connection failed — the server is not reachable.
  """

primitive ConnectionFailedTimeout
  """
  The connection attempt timed out before a TCP or TLS connection was
  established.
  """

primitive ConnectionFailedTimerError
  """
  The connection was aborted because the connect timer's ASIO event
  subscription failed.
  """

primitive ConnectionClosedByServer
  """
  The server closed the TCP connection before the session reached the ready
  state. Distinct from `ConnectionFailedTCP`, which indicates the connection
  could not be established in the first place.

  Applies only to pre-ready peer close. If the server closes the connection
  after the session has reached the ready state, affected queries receive
  `SessionClosed` through their receiver.
  """

primitive SSLServerRefused
  """
  The server refused the SSL request.
  """

primitive TLSHandshakeFailed
  """
  The TLS handshake failed.
  """

primitive TLSAuthFailed
  """
  TLS certificate or authentication verification failed.
  """

primitive UnsupportedAuthenticationMethod
  """
  The server requested an authentication method that this driver does not
  support.
  """

primitive AuthenticationMethodRejected
  """
  The server requested an authentication method that the client's
  `AuthRequirement` policy does not allow. Distinct from
  `UnsupportedAuthenticationMethod`, which indicates the driver cannot
  perform the requested method at all.
  """

primitive ServerVerificationFailed
  """
  The server's SCRAM signature did not match the expected value. This may
  indicate a man-in-the-middle attack or a misconfigured server.
  """

class val InvalidPassword
  """
  SQLSTATE 28P01. The server rejected the provided password. Call
  `response()` to access the full server `ErrorResponseMessage`.
  """
  let _error: ErrorResponseMessage
  new val create(response': ErrorResponseMessage) =>
    _error = response'
  fun val response(): ErrorResponseMessage => _error

class val InvalidAuthorizationSpecification
  """
  SQLSTATE 28000. The server rejected the connection due to an invalid
  authorization specification (nonexistent user, user not permitted to
  connect to the requested database, pg_hba.conf rejection). Call
  `response()` to access the full server `ErrorResponseMessage`.
  """
  let _error: ErrorResponseMessage
  new val create(response': ErrorResponseMessage) =>
    _error = response'
  fun val response(): ErrorResponseMessage => _error

class val TooManyConnections
  """
  SQLSTATE 53300. The server rejected the connection because the maximum
  number of connections has been reached. Call `response()` to access the
  full server `ErrorResponseMessage`.
  """
  let _error: ErrorResponseMessage
  new val create(response': ErrorResponseMessage) =>
    _error = response'
  fun val response(): ErrorResponseMessage => _error

class val InvalidDatabaseName
  """
  SQLSTATE 3D000. The database specified in the connection does not exist.
  Call `response()` to access the full server `ErrorResponseMessage`.
  """
  let _error: ErrorResponseMessage
  new val create(response': ErrorResponseMessage) =>
    _error = response'
  fun val response(): ErrorResponseMessage => _error

class val ServerRejected
  """
  Fallback for any other server ErrorResponse during startup. Call
  `response()` to access the full `ErrorResponseMessage`; inspect
  `response().code` (SQLSTATE) to distinguish specific failure modes.
  """
  let _error: ErrorResponseMessage
  new val create(response': ErrorResponseMessage) =>
    _error = response'
  fun val response(): ErrorResponseMessage => _error

type ConnectionFailureReason is
  ( ConnectionFailedDNS
  | ConnectionFailedTCP
  | ConnectionFailedTimeout
  | ConnectionFailedTimerError
  | ConnectionClosedByServer
  | SSLServerRefused
  | TLSHandshakeFailed
  | TLSAuthFailed
  | UnsupportedAuthenticationMethod
  | AuthenticationMethodRejected
  | ServerVerificationFailed
  | ProtocolViolation
  | InvalidPassword
  | InvalidAuthorizationSpecification
  | TooManyConnections
  | InvalidDatabaseName
  | ServerRejected
  )
