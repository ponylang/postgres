use "buffered"
use "collections"
use "pony_test"
use "random"

// TODO SEAN
// we need tests that verify a chain of messages and that we get the expected
// message type. we could validate the contents as well, but i think for a start
// just validated that we got A, B, C, C, C, D would be good.
// This would provide protection against not reading full messages correctly
// which is currently not covered. For example not handling the null terminator
// from command complete would pass tests herein but would cause the next
// message to incorrectly parse. That isn't currently covered.
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

class \nodoc\ iso _TestResponseParserCommandCompleteMessage is UnitTest
  """
  Verifies expected handling of various command complete messages.
  """
  fun name(): String =>
    "ResponseParser/CommandCompleteMessage"

  fun apply(h: TestHelper) ? =>
    _test_expected(h, "INSERT 1 5", "INSERT", 5)?
    _test_expected(h, "DELETE 18", "DELETE", 18)?
    _test_expected(h, "UPDATE 2047", "UPDATE", 2047)?
    _test_expected(h, "SELECT 5012", "SELECT", 5012)?
    _test_expected(h, "MOVE 11", "MOVE", 11)?
    _test_expected(h, "FETCH 7", "FETCH", 7)?
    _test_expected(h, "COPY 7", "COPY", 7)?
    _test_expected(h, "CREATE TABLE", "CREATE TABLE", 0)?
    _test_expected(h, "DROP TABLE", "DROP TABLE", 0)?
    _test_expected(h, "FUTURE PROOF", "FUTURE PROOF", 0)?
    _test_expected(h, "FUTURE PROOF 2", "FUTURE PROOF", 2)?

    _test_error(h, "")

  fun _test_expected(h: TestHelper, i: String, id: String, value: USize) ? =>
    let bytes = _IncomingCommandCompleteTestMessage(i).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _CommandCompleteMessage =>
      h.assert_eq[String](m.id, id)
      h.assert_eq[USize](m.value, value)
    else
      h.fail("Wrong message returned.")
    end

  fun _test_error(h: TestHelper, i: String) =>
    h.assert_error({() ? =>
      let bytes = _IncomingCommandCompleteTestMessage(i).bytes()
      let r: Reader = Reader.>append(bytes)

    _ResponseParser(r)? }, ("Assert error failed for " + i))

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

class \nodoc\ iso _TestResponseParserEmptyQueryResponseMessage is UnitTest
  """
  Test that we parse incoming empty query response messages correctly.
  """
  fun name(): String =>
    "ResponseParser/EmptyQueryResponseMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingEmptyQueryResponseTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _EmptyQueryResponseMessage =>
      // All good!
      None
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserDataRowMessage is UnitTest
  """
  Test that we parse incoming data row messages correctly.
  """
  fun name(): String =>
    "ResponseParser/DataRowMessage"

  fun apply(h: TestHelper) ? =>
    let columns: Array[(String | None)] val = recover val
      Array[(String | None)]
        .>push("Hello")
        .>push("There")
        .>push(None)
        .>push("")
    end

    let bytes = _IncomingDataRowTestMessage(columns)?.bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _DataRowMessage =>
      h.assert_eq[USize](4, m.columns.size())
      match m.columns(0)?
      | "Hello" => None
      else
        h.fail("First column not parsed correctly")
      end
      match m.columns(1)?
      | "There" => None
      else
        h.fail("Second column not parsed correctly")
      end
      match m.columns(2)?
      | None => None
      else
        h.fail("NULL column not parsed correctly")
      end
      match m.columns(3)?
      | "" => None
      else
        h.fail("Empty string column not parsed correctly")
      end
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ val _IncomingAuthenticationOkTestMessage
    let _bytes: Array[U8] val

    new val create() =>
      let wb: Writer = Writer
      wb.u8(_MessageType.authentication_request())
      wb.u32_be(8)
      wb.i32_be(_AuthenticationRequestType.ok())

      _bytes = WriterToByteArray(wb)

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

    _bytes = WriterToByteArray(wb)

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

    _bytes = WriterToByteArray(wb)

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

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingReadyForQueryTestMessage
  let _bytes: Array[U8] val

  new val create(status: U8) =>
    let wb: Writer = Writer
    wb.u8(_MessageType.ready_for_query())
    wb.u32_be(5)
    wb.u8(status)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingEmptyQueryResponseTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8(_MessageType.empty_query_response())
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCommandCompleteTestMessage
  let _bytes: Array[U8] val

  new val create(command: String) =>
    let payload_size = 4 + command.size() + 1
    let wb: Writer = Writer
    wb.u8(_MessageType.command_complete())
    wb.u32_be(payload_size.u32())
    wb.write(command)
    wb.u8(0)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingDataRowTestMessage
  let _bytes: Array[U8] val

  new val create(columns: Array[(String | None)] val) ? =>
    let number_of_columns = columns.size()
    var payload_size: USize = 4 + 2
    let wb: Writer = Writer
    wb.u8(_MessageType.data_row())
    // placeholder
    wb.u32_be(0)
    wb.u16_be(number_of_columns.u16())
    for column_index in Range(0, number_of_columns) do
      match columns(column_index)?
      | None =>
        wb.u32_be(-1)
        payload_size = payload_size + 4
      | "" =>
        wb.u32_be(0)
        payload_size = payload_size + 4
      | let c: String =>
        wb.u32_be(c.size().u32())
        wb.write(c)
        payload_size = payload_size + 4 + c.size()
      end
    end

    // bytes with placeholder for length
    let b = WriterToByteArray(wb)
    // bytes for payload
    let pw: Writer = Writer.>u32_be(payload_size.u32())
    let pb = WriterToByteArray(pw)
    // copy in payload size
    _bytes = recover val b.clone().>copy_from(pb, 0, 1, 4) end

  fun bytes(): Array[U8] val =>
    _bytes

primitive WriterToByteArray
  fun apply(writer: Writer): Array[U8] val =>
    recover val
      let out = Array[U8]
      for b in writer.done().values() do
        out.append(b)
      end
      out
    end
