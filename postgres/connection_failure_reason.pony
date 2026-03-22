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

type ConnectionFailureReason is
  (ConnectionFailedDNS | ConnectionFailedTCP |
   SSLServerRefused | TLSAuthFailed | TLSHandshakeFailed |
   ConnectionFailedTimeout)
