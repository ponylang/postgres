primitive _Message
  fun startup(user: String, database: String): Array[U8] val ? =>
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

  fun password(pwd: String): Array[U8] val =>
    // TODO STA: We can know the length ahead of time and size our array
    // appropriately and not use push
    recover val
      let msg: Array[U8] = Array[U8]
      let length = pwd.size().u32() + 5
      msg.push('p')
      ifdef bigendian then
        msg.push_u32(length)
      else
        msg.push_u32(length.bswap())
      end

      msg.append(pwd)
      msg.push(U8(0))
      msg
    end
