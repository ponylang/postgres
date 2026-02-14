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

  fun empty_query_response(): U8 =>
    'I'

  fun error_response(): U8 =>
    'E'

  fun query(): U8 =>
    'Q'

  fun ready_for_query(): U8 =>
    'Z'

  fun backend_key_data(): U8 =>
    'K'

  fun notice_response(): U8 =>
    'N'

  fun notification_response(): U8 =>
    'A'

  fun parameter_status(): U8 =>
    'S'

  fun copy_in_response(): U8 =>
    'G'

  fun copy_out_response(): U8 =>
    'H'

  fun copy_data(): U8 =>
    'd'

  fun copy_done(): U8 =>
    'c'

  fun row_description(): U8 =>
    'T'
