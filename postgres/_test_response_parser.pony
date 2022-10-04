use "buffered"
use "collections"
use "pony_test"

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
    let code = "7669"
    let bytes = _IncomingErrorResponseTestMessage(code).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _ErrorResponseMessage =>
      if m.code != code then
        h.fail("Code not correctly parsed.")
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
    let code = "7669"
    let r: Reader = Reader
    r.append(_IncomingErrorResponseTestMessage(code).bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: _ErrorResponseMessage =>
      if m.code != code then
        h.fail("Code not correctly parsed.")
      end
    else
      h.fail("Wrong message returned for first message.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ val _IncomingAuthenticationOkTestMessage
  new val create() =>
    None

  fun bytes(): Array[U8] val =>
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(8)
    wb.i32_be(_AuthenticationRequestType.ok())

    recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

class \nodoc\ val _IncomingAuthenticationMD5PasswordTestMessage
  let _salt: String

  new val create(salt: String) =>
    _salt = salt

  fun bytes(): Array[U8] val =>
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(12)
    wb.i32_be(_AuthenticationRequestType.md5_password())
    wb.write(_salt)

    recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end

class \nodoc\ val _IncomingErrorResponseTestMessage
  let _code: String

  new val create(code: String) =>
    _code = code

   fun bytes(): Array[U8] val =>
    let payload_size = 4 + 1 + _code.size() + 1 + 1
    let wb: Writer = Writer
    wb.u8(_MessageType.error_response())
    wb.u32_be(payload_size.u32())
    wb.u8(_ErrorResponseField.code())
    wb.write(_code)
    wb.u8(0)
    wb.u8(0)

    recover val
      let out = Array[U8]
      for b in wb.done().values() do
        out.append(b)
      end
      out
    end
