primitive _MessageType
  """
  Code that is the first byte of each message.

  See: https://www.postgresql.org/docs/current/protocol-message-formats.html
  """
  fun authentication_request(): U8 =>
    'R'

  fun command_complete(): U8 =>
    'C'

  fun data_row(): U8 =>
    'D'

  fun error_response(): U8 =>
    'E'

  fun ready_for_query(): U8 =>
    'Z'

  fun query(): U8 =>
    'Q'
