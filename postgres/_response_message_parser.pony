use "buffered"

primitive _ResponseMessageParser
  """
  Processes buffered messages synchronously within a query cycle, yielding
  only after ReadyForQuery. This prevents other behaviors (like `close()`)
  from interleaving between result delivery and query dequeuing, which would
  cause double-delivery of `pg_query_failed`.

  If a callback triggers shutdown, `on_shutdown` clears the read buffer,
  causing the next parse to return `None` and exit the loop naturally.
  """
  fun apply(s: Session ref, readbuf: Reader) =>
    while true do
      try
        match _ResponseParser(readbuf)?
        | let msg: _AuthenticationMD5PasswordMessage =>
          s.state.on_authentication_md5_password(s, msg)
        | _AuthenticationOkMessage =>
          s.state.on_authentication_ok(s)
        | let msg: _AuthenticationSASLMessage =>
          s.state.on_authentication_sasl(s, msg)
        | let msg: _AuthenticationSASLContinueMessage =>
          s.state.on_authentication_sasl_continue(s, msg)
        | let msg: _AuthenticationSASLFinalMessage =>
          s.state.on_authentication_sasl_final(s, msg)
        | _UnsupportedAuthenticationMessage =>
          s.state.on_authentication_failed(s, UnsupportedAuthenticationMethod)
          return
        | let msg: _CommandCompleteMessage =>
          s.state.on_command_complete(s, msg)
        | let msg: _DataRowMessage =>
          s.state.on_data_row(s, msg)
        | let err: ErrorResponseMessage =>
          match err.code
          | "28000" =>
            s.state.on_authentication_failed(s,
              InvalidAuthenticationSpecification)
            return
          | "28P01" =>
            s.state.on_authentication_failed(s, InvalidPassword)
            return
          else
            s.state.on_error_response(s, err)
          end
        | let msg: _ReadyForQueryMessage =>
          s.state.on_ready_for_query(s, msg)
          // ReadyForQuery marks the end of a query cycle. Yield to other
          // actors before processing the next cycle.
          s._process_again()
          return
        | let msg: _RowDescriptionMessage =>
          s.state.on_row_description(s, msg)
        | let msg: _BackendKeyDataMessage =>
          s.state.on_backend_key_data(s, msg)
        | let msg: _NotificationResponseMessage =>
          s.state.on_notification(s, msg)
        | let msg: NoticeResponseMessage =>
          s.state.on_notice(s, msg)
        | let msg: _CopyInResponseMessage =>
          s.state.on_copy_in_response(s, msg)
        | _SkippedMessage =>
          // Known async message (ParameterStatus) — intentionally not routed.
          None
        | let msg: _EmptyQueryResponseMessage =>
          s.state.on_empty_query_response(s)
        | None =>
          // No complete message was found. Stop parsing for now.
          return
        end
      else
        // An unrecoverable error was encountered while parsing. Once that
        // happens, there's no way we are going to be able to figure out how
        // to get the responses back into an understandable state. The only
        // thing we can do is shut the session down.

        s.state.shutdown(s)
        return
      end
    end
