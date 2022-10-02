use "crypto"

// TODO STA: unit test for this
primitive _MD5PasswordFromMessagePayload
  fun apply(message: Array[U8] val, user: String, password: String): String ? =>
    let salt = recover val
      [ message(4)? ; message(5)? ; message(6)? ; message(7)? ]
    end

    // TODO STA
    // We know the final length of everything so we can improve performance
    // by not using `+` to build the string
    ("md5" +
      ToHexString(MD5(
        ToHexString(MD5(password + user)) +
        String.from_array(salt))))
