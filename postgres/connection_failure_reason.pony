primitive ConnectionFailedDNS
  """
  Name resolution failed — the server hostname could not be resolved.
  """

primitive ConnectionFailedTCP
  """
  TCP connection failed — the server is not reachable.
  """

primitive SSLServerRefused
  """
  The server refused the SSL request.
  """

primitive TLSAuthFailed
  """
  TLS certificate or authentication verification failed.
  """

primitive TLSHandshakeFailed
  """
  The TLS handshake failed.
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

type ConnectionFailureReason is
  (ConnectionFailedDNS | ConnectionFailedTCP |
   SSLServerRefused | TLSAuthFailed | TLSHandshakeFailed |
   ConnectionFailedTimeout | ConnectionFailedTimerError)
