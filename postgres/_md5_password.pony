use "ssl/crypto"

primitive _MD5Password
  """
  Constructs a validly formatted Postgres MD5 password.
  """
  fun apply(username: String, password: String, salt: String): String =>
    "md5" + ToHexString(MD5(ToHexString(MD5(password + username)) + salt))
