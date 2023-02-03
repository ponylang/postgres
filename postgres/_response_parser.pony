use "buffered"
use "collections"

type _AuthenticationMessages is
  ( _AuthenticationOkMessage
  | _AuthenticationMD5PasswordMessage )

type _ResponseParserResult is
  ( _AuthenticationMessages
  | _CommandCompleteMessage
  | _DataRowMessage
  | _EmptyQueryResponseMessage
  | _ReadyForQueryMessage
  | _RowDescriptionMessage
  | _UnsupportedMessage
  | ErrorResponseMessage
  | None )

primitive _UnsupportedMessage

primitive _ResponseParser
  """
  Takes a reader that contains buffered responses from a Postgres server and
  extract a single message. To process a full buffer, `apply` should be called in a loop until it returns `None` rather than a message type. The input
  buffer is modified.

  Throws an error if an unrecoverable error is encountered. The session should
  be shut down in response.
  """
  fun apply(buffer: Reader): _ResponseParserResult ? =>
    // The minimum size for any complete message is 6. If we have less than
    // 6 received bytes buffered than there is no point to continuing as we
    // definitely don't have a full message.
    if buffer.size() < 5 then
      return None
    end

    let message_type = buffer.peek_u8(0)?
    if ((message_type < 'A') or (message_type > 'z')) or
      ((message_type > 'Z') and (message_type < 'a'))
    then
      // All message codes are ascii letters. If we get something that isn't
      // one then we know we have junk.
      error
    end

    // postgres sends the payload size as the payload plus the 4 bytes for the
    // descriptive header on the payload. We are calling `payload_size` to be
    // only the payload, not the header as well.
    let payload_size = buffer.peek_u32_be(1)?.usize() - 4
    let message_size = payload_size + 4 + 1

    // The message will be `message_size` in length. If we have less than
    // that then there's no point in continuing.
    if buffer.size() < message_size then
      return None
    end

    match message_type
    | _MessageType.authentication_request() =>
      let auth_type = buffer.peek_i32_be(5)?

      if auth_type == _AuthenticationRequestType.ok() then
        // discard the message and type header
        buffer.skip(message_size)?
        // notify that we are authenticated
        return _AuthenticationOkMessage
      elseif auth_type == _AuthenticationRequestType.md5_password() then
        let salt = String.from_array(
          recover val
            [ buffer.peek_u8(9)?
              buffer.peek_u8(10)?
              buffer.peek_u8(11)?
              buffer.peek_u8(12)? ]
          end)
        // discard the message now that we've extracted the salt.
        buffer.skip(message_size)?

        return _AuthenticationMD5PasswordMessage(salt)
      else
        buffer.skip(message_size)?
        return _UnsupportedMessage
      end
    | _MessageType.error_response() =>
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size)?
      return _error_response(consume payload)?
    | _MessageType.ready_for_query() =>
      // Slide past the header...
      buffer.skip(5)?
      // and only get the status indicator byte
      return _ready_for_query(buffer.u8()?)?
    | _MessageType.command_complete() =>
      // TODO SEAN: this will need tests
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size - 1)?
      // And skip the null terminator
      buffer.skip(1)?
      return _command_complete(consume payload)?
    | _MessageType.data_row() =>
      // TODO needs tests
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size)?
      return _data_row(consume payload)?
    | _MessageType.row_description() =>
       // TODO needs tests
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size)?
      return _row_description(consume payload)?
    | _MessageType.empty_query_response() =>
      // Slide past the header...
      buffer.skip(5)?
      // and there's nothing else
      return _EmptyQueryResponseMessage
    else
      buffer.skip(message_size)?
      return _UnsupportedMessage
    end

  fun _error_response(payload: Array[U8] val): ErrorResponseMessage ? =>
    """
    Parse error response messages.
    """
    var code = ""
    var code_index: USize = 0

    let builder = _ErrorResponseMessageBuilder
    while (payload(code_index)? > 0) do
      let field_type = payload(code_index)?

      // Find the field terminator. All fields are null terminated.
      let null_index = payload.find(0, code_index)?
      let field_index = code_index + 1
      let field_data = String.from_array(recover
          payload.slice(field_index, null_index)
        end)

      match field_type
      | 'S' => builder.severity = field_data
      | 'V' => builder.localized_severity = field_data
      | 'C' => builder.code = field_data
      | 'M' => builder.message = field_data
      | 'D' => builder.detail = field_data
      | 'H' => builder.hint = field_data
      | 'P' => builder.position = field_data
      | 'p' => builder.internal_position = field_data
      | 'q' => builder.internal_query = field_data
      | 'W' => builder.error_where = field_data
      | 's' => builder.schema_name = field_data
      | 't' => builder.table_name = field_data
      | 'c' => builder.column_name = field_data
      | 'd' => builder.data_type_name = field_data
      | 'n' => builder.constraint_name = field_data
      | 'F' => builder.file = field_data
      | 'L' => builder.line = field_data
      | 'R' => builder.line = field_data
      end

      code_index = null_index + 1
    end

    builder.build()?

  fun _data_row(payload: Array[U8] val): _DataRowMessage ? =>
    """
    Parse a data row message.
    """
    let reader: Reader = Reader.>append(payload)
    let number_of_columns = reader.u16_be()?.usize()
    let columns: Array[(String| None)] iso = recover iso
      columns.create(number_of_columns)
    end

    for column_index in Range(0, number_of_columns) do
      let column_length = reader.u32_be()?.usize()
      match column_length
      | -1 =>
        columns.push(None)
      | 0 =>
        columns.push("")
      else
        let column = reader.block(column_length)?
        let column_as_string = String.from_array(consume column)
        columns.push(column_as_string)
      end
    end

    _DataRowMessage(consume columns)

  fun _row_description(payload: Array[U8] val): _RowDescriptionMessage ? =>
    """
    Parse a row description message.
    """
    let reader: Reader = Reader.>append(payload)
    let number_of_columns = reader.u16_be()?.usize()
    let columns: Array[(String, U32)] iso = recover iso
      columns.create(number_of_columns)
    end

    for column_index in Range(0, number_of_columns) do
      // column name is a null terminated string
      let cn = reader.read_until(0)?
      let column_name = String.from_array(consume cn)
      // skip table id (int32) and column attribute number (int16)
      reader.skip(6)?
      // column data type
      let column_data_type = reader.u32_be()?
      // skip remaining 3 fields int16, int32, int16
      reader.skip(8)?
      columns.push((column_name, column_data_type))
    end

    _RowDescriptionMessage(consume columns)

  fun _ready_for_query(status: U8): _ReadyForQueryMessage ? =>
    if (status == 'I') or
      (status == 'T') or
      (status == 'E')
    then
      _ReadyForQueryMessage(status)
    else
      error
    end

  // TODO SEAN this needs tests for known expected types from docs plus
  // DROP TABLE & CREATE TABLE
  fun _command_complete(payload: Array[U8] val): _CommandCompleteMessage ? =>
    """
    Parse a command complete message
    """
    let id = String.from_array(payload)
    let parts = id.split(" ")
    match parts.size()
    | 1 =>
      _CommandCompleteMessage(parts(0)?, 0)
    | 2 =>
        let first = parts(0)?
        let second = parts(1)?
        try
          let value = second.u64()?.usize()
          _CommandCompleteMessage(first, value)
        else
          _CommandCompleteMessage(id, 0)
        end
    | 3 =>
      if parts(0)? == "INSERT" then
        _CommandCompleteMessage(parts(0)?, parts(2)?.u64()?.usize())
      else
        let first = parts(0)?
        let second = parts(1)?
        let third = parts(2)?
        let id' = recover val " ".join([first; second].values()) end
        try
          let value = third.u64()?.usize()
          _CommandCompleteMessage(id', value)
        else
          _CommandCompleteMessage(id', 0)
        end
      end
    else
      _CommandCompleteMessage(id, 0)
    end
