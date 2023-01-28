use "buffered"

primitive _ResponseMessageParser
  fun apply(s: Session ref, readbuf: Reader) =>
    try
      match _ResponseParser(readbuf)?
      | let msg: _AuthenticationMD5PasswordMessage =>
        s.state.on_authentication_md5_password(s, msg)
      | _AuthenticationOkMessage =>
        s.state.on_authentication_ok(s)
      | let msg: _CommandCompleteMessage =>
        s.state.on_command_complete(s, msg)
      | let msg: _DataRowMessage =>
        s.state.on_data_row(s, msg)
      | let err: _ErrorResponseMessage =>
        if (err.code == _ErrorCode.invalid_password())
          or (err.code == _ErrorCode.invalid_authentication_specification())
        then
          let reason = if err.code == _ErrorCode.invalid_password() then
            InvalidPassword
          else
            InvalidAuthenticationSpecification
          end

          s.state.on_authentication_failed(s, reason)
          return
        else
          s.state.on_error_response(s, err)
        end
      | let msg: _ReadyForQueryMessage =>
        s.state.on_ready_for_query(s, msg)
      | let msg: _RowDescriptionMessage =>
        s.state.on_row_description(s, msg)
      | None =>
        // No complete message was found. Stop parsing for now.
        return
      end
    else
      // An unrecoverable error was encountered while parsing. Once that
      // happens, there's no way we are going to be able to figure out how
      // to get the responses back into an understandable state. The only
      // thing we can do is shut s session down.

      s.state.shutdown(s)
      return
    end

    s._process_again()