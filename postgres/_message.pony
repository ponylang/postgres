// TODO STA: need unit tests for each type of message that _Message can generate
primitive _Message
  fun startup(user: String, database: String): Array[U8] val ? =>
    // TODO STA: We can know the length ahead of time and size our array
    // appropriately and not use push and not need a placeholder for packet
    // size
    recover val
      let msg: Array[U8] = Array[U8]
      // Placeholder for packet size
      // This will be set when finish up
      msg.push_u32(0)

      // Add version numbers.
      // The version numbers are in network byte order, thus the endian check.
      ifdef bigendian then
        msg.push_u16(U16(3)) // Major Version Number
        msg.push_u16(U16(0)) // Minor Version Number
      else
        msg.push_u16(U16(3).bswap()) // Major Version Number
        msg.push_u16(U16(0).bswap()) // Minor Version Number
      end

      msg.append("user")
      msg.push(0)
      msg.append(user)
      msg.push(0)

      msg.append("database")
      msg.push(0)
      msg.append(database)
      msg.push(0)

      msg.push(0)

      // Set packet size
      // The packet size is in written in network byte order, thus the endian
      // check.
      ifdef bigendian then
        msg.update_u32(0, msg.size().u32())?
      else
        msg.update_u32(0, msg.size().u32().bswap())?
      end
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
