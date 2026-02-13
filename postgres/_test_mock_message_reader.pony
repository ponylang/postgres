use "buffered"

class \nodoc\ _MockMessageReader
  """
  Buffers TCP data and extracts complete PostgreSQL frontend messages.

  `_on_received` fires per TCP segment, not per protocol message. A single
  message can split across segments, or multiple messages can arrive in one
  segment. This class buffers incoming data and provides methods to extract
  complete messages by parsing their length headers.

  Two PostgreSQL frontend message formats are supported:

  - Startup format (StartupMessage, SSLRequest, CancelRequest):
    `Int32(length) payload` — length includes itself.
  - Standard format (Query, Parse, SASLInitialResponse, etc.):
    `Byte1(type) Int32(length) payload` — length includes itself but not
    the type byte.

  The caller decides which format to expect based on its own protocol state.
  Both read methods return the complete message bytes (consuming them from
  the buffer) or `None` if insufficient data is buffered (buffer unchanged).
  """
  let _readbuf: Reader = _readbuf.create()

  fun ref append(data: Array[U8] val) =>
    _readbuf.append(data)

  fun ref read_startup_message(): (Array[U8] val | None) =>
    """
    Try to read a startup-format message. Returns the complete message
    including the 4-byte length prefix, or None if incomplete.
    """
    try
      if _readbuf.size() < 4 then return None end
      let len = _readbuf.peek_u32_be(0)?.usize()
      if _readbuf.size() < len then return None end
      _readbuf.block(len)?
    else
      None
    end

  fun ref read_message(): (Array[U8] val | None) =>
    """
    Try to read a standard-format message. Returns the complete message
    starting with the type byte, or None if incomplete.
    """
    try
      if _readbuf.size() < 5 then return None end
      let len = _readbuf.peek_u32_be(1)?.usize()
      let total = 1 + len
      if _readbuf.size() < total then return None end
      _readbuf.block(total)?
    else
      None
    end
