use "buffered"
use "collections"

type _AuthenticationMessages is
  ( _AuthenticationOkMessage
  | _AuthenticationMD5PasswordMessage
  | _AuthenticationSASLMessage
  | _AuthenticationSASLContinueMessage
  | _AuthenticationSASLFinalMessage
  | _UnsupportedAuthenticationMessage )

type _ResponseParserResult is
  ( _AuthenticationMessages
  | _BackendKeyDataMessage
  | _CommandCompleteMessage
  | _DataRowMessage
  | _EmptyQueryResponseMessage
  | _ReadyForQueryMessage
  | _RowDescriptionMessage
  | _ParseCompleteMessage
  | _BindCompleteMessage
  | _CloseCompleteMessage
  | _NoDataMessage
  | _ParameterDescriptionMessage
  | _NotificationResponseMessage
  | _CopyInResponseMessage
  | _PortalSuspendedMessage
  | _SkippedMessage
  | _UnsupportedMessage
  | ErrorResponseMessage
  | NoticeResponseMessage
  | None )

primitive _SkippedMessage
  """
  Returned by the parser for known PostgreSQL asynchronous message types that
  the driver recognizes but intentionally does not process: ParameterStatus.
  These can arrive between any other messages and are safely ignored.

  Distinct from `_UnsupportedMessage`, which represents truly unknown message
  types that the parser does not recognize at all.
  """

primitive _UnsupportedMessage
  """
  Returned by the parser for message types that are not recognized. This
  represents truly unknown messages — not messages the driver intentionally
  skips (those return `_SkippedMessage`). A future PostgreSQL version could
  introduce new message types that would hit this path.
  """

primitive _ResponseParser
  """
  Takes a reader that contains buffered responses from a Postgres server and
  extract a single message. To process a full buffer, `apply` should be called in a loop until it returns `None` rather than a message type. The input
  buffer is modified.

  Throws an error if an unrecoverable error is encountered. The session should
  be shut down in response.
  """
  fun apply(buffer: Reader): _ResponseParserResult ? =>
    // The minimum size for any complete message is 5. If we have less than
    // 5 received bytes buffered than there is no point to continuing as we
    // definitely don't have a full message.
    if buffer.size() < 5 then
      return None
    end

    let message_type = buffer.peek_u8(0)?
    // Digits ('0'-'9') are accepted for ParseComplete('1'),
    // BindComplete('2'), CloseComplete('3'). Uppercase and lowercase ASCII
    // letters cover all other valid PostgreSQL backend message types.
    if (message_type < '0') or (message_type > 'z') or
      ((message_type > '9') and (message_type < 'A')) or
      ((message_type > 'Z') and (message_type < 'a'))
    then
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
      elseif auth_type == _AuthenticationRequestType.sasl() then
        // Skip past header (5 bytes) and auth type field (4 bytes)
        buffer.skip(9)?
        // Parse null-terminated mechanism names from the remaining payload.
        // The list ends with a lone null byte (empty string).
        let remaining = payload_size - 4
        let payload = buffer.block(remaining)?
        let reader: Reader = Reader.>append(consume payload)
        let mechanisms: Array[String] iso = recover iso Array[String] end
        while reader.size() > 0 do
          let name_bytes = reader.read_until(0)?
          if name_bytes.size() == 0 then
            break
          end
          mechanisms.push(String.from_array(consume name_bytes))
        end
        return _AuthenticationSASLMessage(consume mechanisms)
      elseif auth_type == _AuthenticationRequestType.sasl_continue() then
        // Skip past header (5 bytes) and auth type field (4 bytes)
        buffer.skip(9)?
        let remaining = payload_size - 4
        let data = buffer.block(remaining)?
        return _AuthenticationSASLContinueMessage(consume data)
      elseif auth_type == _AuthenticationRequestType.sasl_final() then
        // Skip past header (5 bytes) and auth type field (4 bytes)
        buffer.skip(9)?
        let remaining = payload_size - 4
        let data = buffer.block(remaining)?
        return _AuthenticationSASLFinalMessage(consume data)
      else
        buffer.skip(message_size)?
        return _UnsupportedAuthenticationMessage
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
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size - 1)?
      // And skip the null terminator
      buffer.skip(1)?
      return _command_complete(consume payload)?
    | _MessageType.data_row() =>
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let payload = buffer.block(payload_size)?
      return _data_row(consume payload)?
    | _MessageType.row_description() =>
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
    | '1' =>
      buffer.skip(message_size)?
      return _ParseCompleteMessage
    | '2' =>
      buffer.skip(message_size)?
      return _BindCompleteMessage
    | '3' =>
      buffer.skip(message_size)?
      return _CloseCompleteMessage
    | 'n' =>
      buffer.skip(message_size)?
      return _NoDataMessage
    | 's' =>
      buffer.skip(message_size)?
      return _PortalSuspendedMessage
    | 't' =>
      // Slide past the header...
      buffer.skip(5)?
      // and parse the parameter description payload
      let payload = buffer.block(payload_size)?
      return _parameter_description(consume payload)?
    | _MessageType.backend_key_data() =>
      buffer.skip(5)?
      let process_id = buffer.i32_be()?
      let secret_key = buffer.i32_be()?
      return _BackendKeyDataMessage(process_id, secret_key)
    | _MessageType.parameter_status() =>
      // Known async message — skip payload without parsing
      buffer.skip(message_size)?
      return _SkippedMessage
    | _MessageType.notice_response() =>
      // Slide past the header...
      buffer.skip(5)?
      // and only get the payload
      let notice_payload = buffer.block(payload_size)?
      return _notice_response(consume notice_payload)?
    | _MessageType.notification_response() =>
      // Slide past the header...
      buffer.skip(5)?
      // and parse the notification payload in an isolated reader
      let notification_payload = buffer.block(payload_size)?
      return _notification_response(consume notification_payload)?
    | _MessageType.copy_in_response() =>
      // Slide past the header...
      buffer.skip(5)?
      // and parse the CopyInResponse payload in an isolated reader
      let copy_payload = buffer.block(payload_size)?
      return _copy_in_response(consume copy_payload)?
    else
      buffer.skip(message_size)?
      return _UnsupportedMessage
    end

  fun _parse_response_fields(payload: Array[U8] val)
    : _ResponseFieldBuilder ?
  =>
    """
    Parse the field list shared by ErrorResponse and NoticeResponse messages.
    """
    var index: USize = 0

    let builder = _ResponseFieldBuilder
    while (payload(index)? > 0) do
      let field_type = payload(index)?

      // Find the field terminator. All fields are null terminated.
      let null_index = payload.find(0, index)?
      let field_index = index + 1
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
      | 'R' => builder.routine = field_data
      end

      index = null_index + 1
    end

    builder

  fun _error_response(payload: Array[U8] val): ErrorResponseMessage ? =>
    """
    Parse error response messages.
    """
    _parse_response_fields(payload)?.build_error()?

  fun _notice_response(payload: Array[U8] val): NoticeResponseMessage ? =>
    """
    Parse notice response messages.
    """
    _parse_response_fields(payload)?.build_notice()?

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
      let column_length = reader.u32_be()?
      match column_length
      | -1 =>
        columns.push(None)
      | 0 =>
        columns.push("")
      else
        let column = reader.block(column_length.usize())?
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

  fun _command_complete(payload: Array[U8] val): _CommandCompleteMessage ? =>
    """
    Parse a command complete message
    """
    let id = String.from_array(payload)
    if id.size() == 0 then
      error
    end

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

  fun _notification_response(payload: Array[U8] val)
    : _NotificationResponseMessage ?
  =>
    """
    Parse a notification response message.
    """
    let reader: Reader = Reader.>append(payload)
    let pid = reader.i32_be()?
    let channel_bytes = reader.read_until(0)?
    let channel = String.from_array(consume channel_bytes)
    let payload_bytes = reader.read_until(0)?
    let payload' = String.from_array(consume payload_bytes)
    _NotificationResponseMessage(pid, channel, payload')

  fun _copy_in_response(payload: Array[U8] val)
    : _CopyInResponseMessage ?
  =>
    """
    Parse a CopyInResponse message.
    """
    let reader: Reader = Reader.>append(payload)
    let format = reader.u8()?
    let num_cols = reader.u16_be()?.usize()
    let col_fmts: Array[U8] iso = recover iso Array[U8](num_cols) end

    for i in Range(0, num_cols) do
      col_fmts.push(reader.u16_be()?.u8())
    end

    _CopyInResponseMessage(format, consume col_fmts)

  fun _parameter_description(payload: Array[U8] val)
    : _ParameterDescriptionMessage ?
  =>
    """
    Parse a parameter description message.
    """
    let reader: Reader = Reader.>append(payload)
    let num_params = reader.u16_be()?.usize()
    let oids: Array[U32] iso = recover iso Array[U32](num_params) end

    for i in Range(0, num_params) do
      oids.push(reader.u32_be()?)
    end

    _ParameterDescriptionMessage(consume oids)
