primitive _Message
  fun startup(user: String, database: String): Array[U8] val =>
    try
      recover val
        // 4 + 4 + 4 + 1 + user.size() + 1 + 8 + 1 + database.size() + 1 + 1
        let length = 25 + user.size() + database.size()
        let msg: Array[U8] = Array[U8].init(0, length)
        // Placeholder for packet size
        // This will be set when finish up
        ifdef bigendian then
          msg.update_u32(0, length.u32())?
        else
          msg.update_u32(0, length.u32().bswap())?
        end

        // Add version numbers.
        // The version numbers are in network byte order, thus the endian check.
        ifdef bigendian then
          msg.update_u16(4, U16(3))? // Major Version Number
          msg.update_u16(6, U16(0))? // Minor Version Number
        else
          msg.update_u16(4, U16(3).bswap())? // Major Version Number
          msg.update_u16(6, U16(0).bswap())? // Minor Version Number
        end

        msg.copy_from("user".array(), 0, 8, 4)
        // space for null left here at byte 13
        msg.copy_from(user.array(), 0, 13, user.size())
        // space for null left here at byte 13 + user.size() + 1

        msg.copy_from("database".array(), 0, 14 + user.size(), 8)
        // space for null left here at byte 14 + user.size() + 8 + 1
        msg.copy_from(database.array(), 0, 23 + user.size(), database.size())
        // space for null left here at
        // space for null left here at
        msg
      end
    else
      _Unreachable()
      []
    end

  fun password(pwd: String): Array[U8] val =>
    try
      recover val
        let payload_length = pwd.size().u32() + 5
        let msg_length =  (payload_length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_length)
        msg.update_u8(0, 'p')?
        ifdef bigendian then
          msg.update_u32(1, payload_length)?
        else
          msg.update_u32(1, payload_length.bswap())?
        end
        msg.copy_from(pwd.array(), 0, 5, pwd.size())
        //  space for null left here
        msg
      end
    else
      _Unreachable()
      []
    end
