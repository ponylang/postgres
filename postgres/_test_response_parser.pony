use "buffered"
use "collections"
use "pony_test"
use "random"

class \nodoc\ iso _TestResponseParserEmptyBuffer is UnitTest
  """
  Verify that handling an empty buffer to the parser returns `None`
  """
  fun name(): String =>
    "ResponseParser/EmptyBuffer"

  fun apply(h: TestHelper) ? =>
    let empty: Reader = Reader

    if _ResponseParser(empty)? isnt None then
      h.fail()
    end

class \nodoc\ iso _TestResponseParserIncompleteMessage is UnitTest
  """
  Verify that handing a buffer that isn't a complete message to the parser
  returns `None`
  """
  fun name(): String =>
    "ResponseParser/IncompleteMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingAuthenticationOkTestMessage.bytes()
    let complete_message_index = bytes.size()

    for i in Range(0, complete_message_index) do
      let r: Reader = Reader
      let s: Array[U8] val = bytes.trim(0, i)
      r.append(s)

      if _ResponseParser(r)? isnt None then
        h.fail(
          "Parsing incomplete message with size of " +
          i.string() +
          " didn't return None.")
      end
    end

class \nodoc\ iso _TestResponseParserJunkMessage is UnitTest
  """
  Verify that handing a buffer contains "junk" data leads to an error.
  """
  fun name(): String =>
    "ResponseParser/JunkMessage"

  fun apply(h: TestHelper) =>
    h.assert_error({() ? =>
      let bytes = _IncomingJunkTestMessage.bytes()
      let r: Reader = Reader
      r.append(bytes)
      _ResponseParser(r)? })

class \nodoc\ iso _TestResponseParserAuthenticationOkMessage is UnitTest
  """
  Verify that AuthenticationOk messages are parsed correctly
  """
  fun name(): String =>
    "ResponseParser/AuthenticationOkMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingAuthenticationOkTestMessage.bytes()
    let r: Reader = Reader
    r.append(bytes)

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail()
    end

class \nodoc\ iso _TestResponseParserAuthenticationMD5PasswordMessage is UnitTest
  """
  Verify that AuthenticationMD5Password messages are parsed correctly
  """
  fun name(): String =>
    "ResponseParser/AuthenticationMD5PasswordMessage"

  fun apply(h: TestHelper) ? =>
    let salt = "7669"
    let bytes = _IncomingAuthenticationMD5PasswordTestMessage(salt).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _AuthenticationMD5PasswordMessage =>
      if m.salt != salt then
        h.fail("Salt not correctly parsed.")
      end
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserErrorResponseMessage is UnitTest
  """
  Verify that ErrorResponse messages are parsed correctly
  """
  fun name(): String =>
    "ResponseParser/ErrorResponseMessage"

  fun apply(h: TestHelper) ? =>
    let severity = "ERROR"
    let code = "7669"
    let message = "Who's gonna die when the old database dies?"
    let bytes =
      _IncomingErrorResponseTestMessage(severity, code, message).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: ErrorResponseMessage =>
      if m.severity != severity then
        h.fail("Severity not correctly parsed.")
      end
      if m.code != code then
        h.fail("Code not correctly parsed.")
      end
      if m.message != message then
        h.fail("Message not correctly parsed.")
      end
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesAuthenticationOkFirst
  is UnitTest
  """
  Verify that we correctly advance forward from an authentication ok message
  such that it doesn't corrupt the buffer and lead to an incorrect result for
  the next message.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/AuthenticationOkFirst"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingAuthenticationOkTestMessage.bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for first message.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ iso
  _TestResponseParserMultipleMessagesAuthenticationMD5PasswordFirst
  is UnitTest
  """
  Verify that we correctly advance forward from an authentication md5 password message such that it doesn't corrupt the buffer and lead to an incorrect
  result for the next message.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/AuthenticationMD5PasswordFirst"

  fun apply(h: TestHelper) ? =>
    let salt = "7669"
    let r: Reader = Reader
    r.append(_IncomingAuthenticationMD5PasswordTestMessage(salt).bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: _AuthenticationMD5PasswordMessage =>
      if m.salt != salt then
        h.fail("Salt not correctly parsed.")
      end
    else
      h.fail("Wrong message returned for first message.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesErrorResponseFirst is UnitTest
  """
  Verify that we correctly advance forward from an error response message such
  that it doesn't corrupt the buffer and lead to an incorrect result for the
  next message.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/ErrorResponseFirst"

  fun apply(h: TestHelper) ? =>
    let severity = "ERROR"
    let code = "7669"
    let message = "Who's gonna die when the old database dies?"
    let r: Reader = Reader
    r.append(_IncomingErrorResponseTestMessage(severity, code, message).bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: ErrorResponseMessage =>
      if m.severity != severity then
        h.fail("Severity not correctly parsed.")
      end
      if m.code != code then
        h.fail("Code not correctly parsed.")
      end
      if m.message != message then
        h.fail("Message not correctly parsed.")
      end
    else
      h.fail("Wrong message returned for first message.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ iso _TestResponseParserReadyForQueryMessage is UnitTest
  """
  Test that we parse incoming ready for query messages correctly.
  """
  fun name(): String =>
    "ResponseParser/ReadyForQueryMessage"

  fun apply(h: TestHelper) ? =>
    _idle_test(h)?
    _in_transaction_block_test(h)?
    _failed_transaction_test(h)?
    _bunk(h)

  fun _idle_test(h: TestHelper) ? =>
    let s: U8 = 'I'
    let bytes =
      _IncomingReadyForQueryTestMessage(s).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      if not m.idle() then
        h.fail("Incorrect status.")
      end
    else
      h.fail("Wrong message returned.")
    end

  fun _in_transaction_block_test(h: TestHelper) ? =>
    let s: U8 = 'T'
    let bytes =
      _IncomingReadyForQueryTestMessage(s).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      if not m.in_transaction_block() then
        h.fail("Incorrect status.")
      end
    else
      h.fail("Wrong message returned.")
    end

  fun _failed_transaction_test(h: TestHelper) ? =>
    let s: U8 = 'E'
    let bytes =
      _IncomingReadyForQueryTestMessage(s).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      if not m.failed_transaction() then
        h.fail("Incorrect status.")
      end
    else
      h.fail("Wrong message returned.")
    end

  fun _bunk(h: TestHelper) =>
    h.assert_error({() ? =>
      let s: U8 = 'A'
      let bytes =
        _IncomingReadyForQueryTestMessage(s).bytes()
      let r: Reader = Reader
      r.append(bytes)

      _ResponseParser(r)? })

class \nodoc\ val _IncomingAuthenticationOkTestMessage
    let _bytes: Array[U8] val

    new val create() =>
      let wb: Writer = Writer
      wb.u8(_MessageType.authentication_request())
      wb.u32_be(8)
      wb.i32_be(_AuthenticationRequestType.ok())

      _bytes = recover val
        let out = Array[U8]
        for b in wb.done().values() do
          out.append(b)
        end
        out
      end

    fun bytes(): Array[U8] val =>
      _bytes

class \nodoc\ val _IncomingAuthenticationMD5PasswordTestMessage
  let _bytes: Array[U8] val

  new val create(salt: String) =>
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(12)
    wb.i32_be(_AuthenticationRequestType.md5_password())
    wb.write(salt)

    _bytes = recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingErrorResponseTestMessage
  let _bytes: Array[U8] val

  new val create(severity: String, code: String, message: String) =>
    let payload_size = 4 +
      1 + severity.size() + 1 +
      1 + code.size() + 1 +
      1 + message.size() + 1 +
      1

    let wb: Writer = Writer
    wb.u8(_MessageType.error_response())
    wb.u32_be(payload_size.u32())
    wb.u8('S')
    wb.write(severity)
    wb.u8(0)
    wb.u8('C')
    wb.write(code)
    wb.u8(0)
    wb.u8('M')
    wb.write(message)
    wb.u8(0)
    wb.u8(0)

    _bytes = recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingJunkTestMessage
  """
  Creates a junk message where "junk" is currently defined as having a message
  type that we don't recognize, aka not an ascii letter.
  """
  let _bytes: Array[U8] val

  new val create() =>
    let rand = Rand
    let wb: Writer = Writer
    wb.u8(1)
    wb.u32_be(7669)
    for i in Range(0, 100_000) do
      wb.u32_be(rand.u32())
    end

    _bytes = recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingReadyForQueryTestMessage
  let _bytes: Array[U8] val

  new val create(status: U8) =>
    let wb: Writer = Writer
    wb.u8(_MessageType.ready_for_query())
    wb.u32_be(5)
    wb.u8(status)

    _bytes = recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

  fun bytes(): Array[U8] val =>
    _bytes
