primitive _FrontendMessage
  fun startup(user: String, database: String): Array[U8] val =>
    try
      recover val
        // 4 + 4 + 4 + 1 + user.size() + 1 + 8 + 1 + database.size() + 1 + 1
        let length = 25 + user.size() + database.size()
        let msg: Array[U8] = Array[U8].init(0, length)
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

  fun query(string: String): Array[U8] val =>
    try
      recover val
        // 1 + 4 + string.size()
        let payload_length = string.size().u32() + 5
        let msg_length = (payload_length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_length)
        msg.update_u8(0, _MessageType.query())?
        ifdef bigendian then
          msg.update_u32(1, payload_length)?
        else
          msg.update_u32(1, payload_length.bswap())?
        end
        msg.copy_from(string.array(), 0, 5, string.size())
        //  space for null left here
        msg
      end
    else
      _Unreachable()
      []
    end

  fun parse(name: String, query_string: String,
    param_type_oids: Array[U32] val): Array[U8] val
  =>
    """
    Build a Parse message for the extended query protocol.

    Format: Byte1('P') Int32(len) String(name) String(query)
            Int16(num_param_types) Int32[](param_type_oids)
    """
    try
      recover val
        // 1 type + 4 length + name + null + query + null + 2 num_params
        // + (4 * num_oids)
        let length: U32 = 4 + name.size().u32() + 1
          + query_string.size().u32() + 1 + 2
          + (param_type_oids.size().u32() * 4)
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'P')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        var offset: USize = 5
        msg.copy_from(name.array(), 0, offset, name.size())
        offset = offset + name.size() + 1 // null terminator from init
        msg.copy_from(query_string.array(), 0, offset, query_string.size())
        offset = offset + query_string.size() + 1
        ifdef bigendian then
          msg.update_u16(offset, param_type_oids.size().u16())?
        else
          msg.update_u16(offset, param_type_oids.size().u16().bswap())?
        end
        offset = offset + 2
        for oid in param_type_oids.values() do
          ifdef bigendian then
            msg.update_u32(offset, oid)?
          else
            msg.update_u32(offset, oid.bswap())?
          end
          offset = offset + 4
        end
        msg
      end
    else
      _Unreachable()
      []
    end

  fun bind(portal: String, stmt: String,
    params: Array[(String | None)] val): Array[U8] val
  =>
    """
    Build a Bind message for the extended query protocol.

    Format: Byte1('B') Int32(len) String(portal) String(stmt)
            Int16(0) Int16(num_params) [Int32(val_len) Byte[](val)]*
            Int16(0)

    All parameters use text format (num_param_formats = 0 shorthand).
    NULL parameters have val_len = -1 with no value bytes.
    """
    try
      recover val
        // Calculate params payload size
        var params_size: USize = 0
        for p in params.values() do
          params_size = params_size + 4 // val_len field
          match p
          | let s: String => params_size = params_size + s.size()
          end
        end
        // 1 type + 4 length + portal + null + stmt + null
        // + 2 num_param_formats + 2 num_params + params_size
        // + 2 num_result_formats
        let length: U32 = 4 + portal.size().u32() + 1 + stmt.size().u32() + 1
          + 2 + 2 + params_size.u32() + 2
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'B')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        var offset: USize = 5
        msg.copy_from(portal.array(), 0, offset, portal.size())
        offset = offset + portal.size() + 1
        msg.copy_from(stmt.array(), 0, offset, stmt.size())
        offset = offset + stmt.size() + 1
        // num_param_formats = 0 (all text shorthand) — already zero from init
        offset = offset + 2
        // num_params
        ifdef bigendian then
          msg.update_u16(offset, params.size().u16())?
        else
          msg.update_u16(offset, params.size().u16().bswap())?
        end
        offset = offset + 2
        for p in params.values() do
          match p
          | let s: String =>
            ifdef bigendian then
              msg.update_u32(offset, s.size().u32())?
            else
              msg.update_u32(offset, s.size().u32().bswap())?
            end
            offset = offset + 4
            msg.copy_from(s.array(), 0, offset, s.size())
            offset = offset + s.size()
          | None =>
            // NULL: val_len = -1 (0xFFFFFFFF as unsigned)
            ifdef bigendian then
              msg.update_u32(offset, U32.max_value())?
            else
              msg.update_u32(offset, U32.max_value().bswap())?
            end
            offset = offset + 4
          end
        end
        // num_result_formats = 0 (all text shorthand) — already zero from init
        msg
      end
    else
      _Unreachable()
      []
    end

  fun describe_portal(portal: String): Array[U8] val =>
    """
    Build a Describe message for a portal.

    Format: Byte1('D') Int32(len) Byte1('P') String(portal)
    """
    try
      recover val
        // 1 type + 4 length + 1 indicator + portal + null
        let length: U32 = 4 + 1 + portal.size().u32() + 1
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'D')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        msg.update_u8(5, 'P')?
        msg.copy_from(portal.array(), 0, 6, portal.size())
        msg
      end
    else
      _Unreachable()
      []
    end

  fun describe_statement(name: String): Array[U8] val =>
    """
    Build a Describe message for a prepared statement.

    Format: Byte1('D') Int32(len) Byte1('S') String(name)
    """
    try
      recover val
        // 1 type + 4 length + 1 indicator + name + null
        let length: U32 = 4 + 1 + name.size().u32() + 1
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'D')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        msg.update_u8(5, 'S')?
        msg.copy_from(name.array(), 0, 6, name.size())
        msg
      end
    else
      _Unreachable()
      []
    end

  fun close_statement(name: String): Array[U8] val =>
    """
    Build a Close message for a prepared statement.

    Format: Byte1('C') Int32(len) Byte1('S') String(name)
    """
    try
      recover val
        // 1 type + 4 length + 1 indicator + name + null
        let length: U32 = 4 + 1 + name.size().u32() + 1
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'C')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        msg.update_u8(5, 'S')?
        msg.copy_from(name.array(), 0, 6, name.size())
        msg
      end
    else
      _Unreachable()
      []
    end

  fun execute_msg(portal: String, max_rows: U32): Array[U8] val =>
    """
    Build an Execute message.

    Format: Byte1('E') Int32(len) String(portal) Int32(max_rows)
    """
    try
      recover val
        // 1 type + 4 length + portal + null + 4 max_rows
        let length: U32 = 4 + portal.size().u32() + 1 + 4
        let msg_size = (length + 1).usize()
        let msg: Array[U8] = Array[U8].init(0, msg_size)
        msg.update_u8(0, 'E')?
        ifdef bigendian then
          msg.update_u32(1, length)?
        else
          msg.update_u32(1, length.bswap())?
        end
        msg.copy_from(portal.array(), 0, 5, portal.size())
        let max_rows_offset = 5 + portal.size() + 1
        ifdef bigendian then
          msg.update_u32(max_rows_offset, max_rows)?
        else
          msg.update_u32(max_rows_offset, max_rows.bswap())?
        end
        msg
      end
    else
      _Unreachable()
      []
    end

  fun ssl_request(): Array[U8] val =>
    """
    Build an SSLRequest message.

    Format: Int32(8) Int32(80877103) — no message type byte, same pattern as
    startup(). The magic number 80877103 = 1234 << 16 | 5679.
    """
    try
      recover val
        let msg: Array[U8] = Array[U8].init(0, 8)
        ifdef bigendian then
          msg.update_u32(0, U32(8))?
          msg.update_u32(4, U32(80877103))?
        else
          msg.update_u32(0, U32(8).bswap())?
          msg.update_u32(4, U32(80877103).bswap())?
        end
        msg
      end
    else
      _Unreachable()
      []
    end

  fun sync(): Array[U8] val =>
    """
    Build a Sync message.

    Format: Byte1('S') Int32(4)
    """
    try
      recover val
        let msg: Array[U8] = Array[U8].init(0, 5)
        msg.update_u8(0, 'S')?
        ifdef bigendian then
          msg.update_u32(1, U32(4))?
        else
          msg.update_u32(1, U32(4).bswap())?
        end
        msg
      end
    else
      _Unreachable()
      []
    end
