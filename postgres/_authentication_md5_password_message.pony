class val _AuthenticationMD5PasswordMessage
  """
  Message from the backend that indicates that MD5 authentication is being
  requested by the server. Contains a salt needed to construct the reply.
  """
  let salt: String

  new val create(salt': String) =>
    salt = salt'
