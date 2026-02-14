use "buffered"
use "collections"
use "pony_check"
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

class \nodoc\ iso _TestResponseParserRowDescriptionMessage is UnitTest
  """
  Verifies expected handling of various row description messages.
  """
  fun name(): String =>
    "ResponseParser/RowDescriptionMessage"

  fun apply(h: TestHelper) ? =>
    let columns: Array[(String, String)] val = recover val
      [ ("is_it_true", "bool"); ("description", "text"); ("tiny", "int2")
        ("essay", "text"); ("price", "int4"); ("counter", "int8")
        ("money", "float4"); ("big_money", "float8") ]
    end
    let expected: Array[(String, U32)] val = recover val
      [ ("is_it_true", 16); ("description", 25); ("tiny", 21); ("essay", 25)
        ("price", 23); ("counter", 20); ("money", 700); ("big_money", 701) ]
    end

    let bytes = _IncomingRowDescriptionTestMessage(columns)?.bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _RowDescriptionMessage =>
      h.assert_eq[USize](expected.size(), m.columns.size())
      for i in Range(0, expected.size()) do
        h.assert_eq[String](expected(i)?._1, m.columns(i)?._1)
        h.assert_eq[U32](expected(i)?._2, m.columns(i)?._2)
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
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
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
      h.assert_is[TransactionStatus](TransactionInBlock,
        m.transaction_status())
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
      h.assert_is[TransactionStatus](TransactionFailed,
        m.transaction_status())
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

    let bytes = _IncomingDataRowTestMessage(columns).bytes()
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

class \nodoc\ iso _TestResponseParserParseCompleteMessage is UnitTest
  fun name(): String =>
    "ResponseParser/ParseCompleteMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingParseCompleteTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _ParseCompleteMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserBindCompleteMessage is UnitTest
  fun name(): String =>
    "ResponseParser/BindCompleteMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingBindCompleteTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _BindCompleteMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserNoDataMessage is UnitTest
  fun name(): String =>
    "ResponseParser/NoDataMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingNoDataTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _NoDataMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserCloseCompleteMessage is UnitTest
  fun name(): String =>
    "ResponseParser/CloseCompleteMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingCloseCompleteTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _CloseCompleteMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserParameterDescriptionMessage is UnitTest
  fun name(): String =>
    "ResponseParser/ParameterDescriptionMessage"

  fun apply(h: TestHelper) ? =>
    let oids: Array[U32] val = recover val [as U32: 23; 25; 16] end
    let bytes = _IncomingParameterDescriptionTestMessage(oids).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _ParameterDescriptionMessage =>
      h.assert_eq[USize](3, m.param_oids.size())
      h.assert_eq[U32](23, m.param_oids(0)?)
      h.assert_eq[U32](25, m.param_oids(1)?)
      h.assert_eq[U32](16, m.param_oids(2)?)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserPortalSuspendedMessage is UnitTest
  fun name(): String =>
    "ResponseParser/PortalSuspendedMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingPortalSuspendedTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _PortalSuspendedMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserDigitMessageTypeNotJunk is UnitTest
  """
  Verify that digit message types ('1', '2', '3') are accepted by junk
  detection, while characters just below the digit range are still rejected.
  """
  fun name(): String =>
    "ResponseParser/DigitMessageTypeNotJunk"

  fun apply(h: TestHelper) ? =>
    // '1' (ParseComplete) should be accepted
    let bytes = _IncomingParseCompleteTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)
    if _ResponseParser(r)? isnt _ParseCompleteMessage then
      h.fail("Digit '1' was not accepted.")
    end

    // '/' (47, just below '0'=48) should be rejected as junk
    h.assert_error({() ? =>
      let wb: Writer = Writer
      wb.u8('/')
      wb.u32_be(4)
      let junk_bytes = WriterToByteArray(wb)
      let r': Reader = Reader.>append(junk_bytes)
      _ResponseParser(r')? })

class \nodoc\ iso
  _TestResponseParserMultipleMessagesParseCompleteFirst
  is UnitTest
  """
  Verify correct buffer advancement across an extended query response
  sequence: ParseComplete + BindComplete + CommandComplete + ReadyForQuery.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/ParseCompleteFirst"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingParseCompleteTestMessage.bytes())
    r.append(_IncomingBindCompleteTestMessage.bytes())
    r.append(_IncomingCommandCompleteTestMessage("INSERT 0 1").bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    if _ResponseParser(r)? isnt _ParseCompleteMessage then
      h.fail("Wrong message for ParseComplete.")
    end

    if _ResponseParser(r)? isnt _BindCompleteMessage then
      h.fail("Wrong message for BindComplete.")
    end

    match _ResponseParser(r)?
    | let m: _CommandCompleteMessage =>
      h.assert_eq[String]("INSERT", m.id)
      h.assert_eq[USize](1, m.value)
    else
      h.fail("Wrong message for CommandComplete.")
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for ReadyForQuery.")
    end

class \nodoc\ iso _TestResponseParserBackendKeyDataMessage is UnitTest
  """
  Verify that BackendKeyData messages are parsed correctly, extracting the
  process ID and secret key.
  """
  fun name(): String =>
    "ResponseParser/BackendKeyDataMessage"

  fun apply(h: TestHelper) ? =>
    let pid: I32 = 12345
    let secret: I32 = 67890
    let bytes = _IncomingBackendKeyDataTestMessage(pid, secret).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let m: _BackendKeyDataMessage =>
      h.assert_eq[I32](pid, m.process_id)
      h.assert_eq[I32](secret, m.secret_key)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso
  _TestResponseParserMultipleMessagesBackendKeyDataFirst
  is UnitTest
  """
  Verify correct buffer advancement after a BackendKeyData message followed
  by a ReadyForQuery message.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/BackendKeyDataFirst"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingBackendKeyDataTestMessage(42, 99).bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    match _ResponseParser(r)?
    | let m: _BackendKeyDataMessage =>
      h.assert_eq[I32](42, m.process_id)
      h.assert_eq[I32](99, m.secret_key)
    else
      h.fail("Wrong message returned for first message.")
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ iso _TestResponseParserAuthenticationSASLMessage is UnitTest
  """
  Verify that AuthenticationSASL (type 10) messages are parsed correctly,
  extracting the list of mechanism names.
  """
  fun name(): String =>
    "ResponseParser/AuthenticationSASLMessage"

  fun apply(h: TestHelper) ? =>
    let mechanisms: Array[String] val = recover val ["SCRAM-SHA-256"] end
    let bytes = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLMessage =>
      h.assert_eq[USize](1, m.mechanisms.size())
      h.assert_eq[String]("SCRAM-SHA-256", m.mechanisms(0)?)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserAuthenticationSASLContinueMessage
  is UnitTest
  """
  Verify that AuthenticationSASLContinue (type 11) messages are parsed
  correctly, extracting the raw data payload.
  """
  fun name(): String =>
    "ResponseParser/AuthenticationSASLContinueMessage"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "r=abc123,s=c2FsdA==,i=4096".array()
    let bytes = _IncomingAuthenticationSASLContinueTestMessage(data).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLContinueMessage =>
      h.assert_array_eq[U8](data, m.data)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserAuthenticationSASLFinalMessage
  is UnitTest
  """
  Verify that AuthenticationSASLFinal (type 12) messages are parsed
  correctly, extracting the raw data payload.
  """
  fun name(): String =>
    "ResponseParser/AuthenticationSASLFinalMessage"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "v=dGVzdA==".array()
    let bytes = _IncomingAuthenticationSASLFinalTestMessage(data).bytes()
    let r: Reader = Reader
    r.append(bytes)

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLFinalMessage =>
      h.assert_array_eq[U8](data, m.data)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserUnsupportedAuthenticationMessage
  is UnitTest
  """
  Verify that an authentication request with an unsupported type (e.g.,
  cleartext password, type 3) is parsed as _UnsupportedAuthenticationMessage.
  """
  fun name(): String =>
    "ResponseParser/UnsupportedAuthenticationMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingUnsupportedAuthenticationTestMessage(3).bytes()
    let r: Reader = Reader
    r.append(bytes)

    if _ResponseParser(r)? isnt _UnsupportedAuthenticationMessage then
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesSASLFirst is UnitTest
  """
  Verify correct buffer advancement from an AuthenticationSASL message
  followed by an AuthenticationOk message.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/AuthenticationSASLFirst"

  fun apply(h: TestHelper) ? =>
    let mechanisms: Array[String] val = recover val ["SCRAM-SHA-256"] end
    let r: Reader = Reader
    r.append(
      _IncomingAuthenticationSASLTestMessage(mechanisms).bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLMessage =>
      h.assert_eq[USize](1, m.mechanisms.size())
    else
      h.fail("Wrong message returned for first message.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message returned for second message.")
    end

class \nodoc\ val _IncomingAuthenticationSASLTestMessage
  let _bytes: Array[U8] val

  new val create(mechanisms: Array[String] val) =>
    // Auth type 10. Payload: 4 bytes auth type + mechanism names (each null-
    // terminated) + terminating null byte.
    var mechanism_size: USize = 0
    for m in mechanisms.values() do
      mechanism_size = mechanism_size + m.size() + 1
    end
    mechanism_size = mechanism_size + 1 // terminating null

    let payload_size: U32 = (4 + 4 + mechanism_size).u32()
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(payload_size)
    wb.i32_be(_AuthenticationRequestType.sasl())
    for m in mechanisms.values() do
      wb.write(m)
      wb.u8(0)
    end
    wb.u8(0) // list terminator

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingAuthenticationSASLContinueTestMessage
  let _bytes: Array[U8] val

  new val create(data: Array[U8] val) =>
    let payload_size: U32 = (4 + 4 + data.size()).u32()
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(payload_size)
    wb.i32_be(_AuthenticationRequestType.sasl_continue())
    wb.write(data)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingAuthenticationSASLFinalTestMessage
  let _bytes: Array[U8] val

  new val create(data: Array[U8] val) =>
    let payload_size: U32 = (4 + 4 + data.size()).u32()
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(payload_size)
    wb.i32_be(_AuthenticationRequestType.sasl_final())
    wb.write(data)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingUnsupportedAuthenticationTestMessage
  """
  Constructs a raw authentication request message with an unsupported auth
  type. The wire format is the same as AuthenticationOk (8-byte payload with
  just the auth type field), since unsupported types have no additional data
  the driver needs to parse.
  """
  let _bytes: Array[U8] val

  new val create(auth_type: I32) =>
    let wb: Writer = Writer
    wb.u8(_MessageType.authentication_request())
    wb.u32_be(8)
    wb.i32_be(auth_type)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingBackendKeyDataTestMessage
  let _bytes: Array[U8] val

  new val create(process_id: I32, secret_key: I32) =>
    let wb: Writer = Writer
    wb.u8(_MessageType.backend_key_data())
    wb.u32_be(12)
    wb.i32_be(process_id)
    wb.i32_be(secret_key)

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

  new val create(columns: Array[(String | None)] val) =>
    let number_of_columns = columns.size()
    var payload_size: USize = 4 + 2
    let wb: Writer = Writer
    wb.u8(_MessageType.data_row())
    // placeholder
    wb.u32_be(0)
    wb.u16_be(number_of_columns.u16())
    for column in columns.values() do
      match column
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

class \nodoc\ val _IncomingRowDescriptionTestMessage
  let _bytes: Array[U8] val

  new val create(columns: Array[(String, String)] val) ? =>
    let number_of_columns = columns.size()
    var payload_size: USize = 4 + 2
    let wb: Writer = Writer
    wb.u8(_MessageType.row_description())
    // placeholder
    wb.u32_be(0)
    wb.u16_be(number_of_columns.u16())
    for column in columns.values() do
      let name: String = column._1
      let column_type: U32 = match column._2
        | "text" => 25
        | "bytea" => 17
        | "bool" => 16
        | "int2" => 21
        | "int4" => 23
        | "int8" => 20
        | "float4" => 700
        | "float8" => 701
        else
          error
        end
      // column name size and null terminator plus additional fields
      payload_size = payload_size + name.size() + 1 + 18
      wb.write(name)
      wb.u8(0)
      // currently unused in the parser
      wb.u32_be(0)
      // currently unused in the parser
      wb.u16_be(0)
      wb.u32_be(column_type)
      // currently unused in the parser
      wb.u16_be(0)
      wb.u32_be(0)
      wb.u16_be(0)
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

class \nodoc\ val _IncomingParseCompleteTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8('1')
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingBindCompleteTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8('2')
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingNoDataTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8('n')
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCloseCompleteTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8('3')
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingParameterDescriptionTestMessage
  let _bytes: Array[U8] val

  new val create(oids: Array[U32] val) =>
    let payload_size = 4 + 2 + (oids.size() * 4)
    let wb: Writer = Writer
    wb.u8('t')
    wb.u32_be(payload_size.u32())
    wb.u16_be(oids.size().u16())
    for oid in oids.values() do
      wb.u32_be(oid)
    end

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingPortalSuspendedTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8('s')
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ iso _TestResponseParserCopyInResponseMessage is UnitTest
  """
  Verify that CopyInResponse ('G') messages are parsed into
  _CopyInResponseMessage with correct fields.
  """
  fun name(): String =>
    "ResponseParser/CopyInResponseMessage"

  fun apply(h: TestHelper) ? =>
    // Text format (0) with 3 columns (each text format)
    let col_fmts: Array[U8] val = recover val [as U8: 0; 0; 0] end
    let bytes = _IncomingCopyInResponseTestMessage(0, col_fmts).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: _CopyInResponseMessage =>
      h.assert_eq[U8](0, msg.format)
      h.assert_eq[USize](3, msg.column_formats.size())
      h.assert_eq[U8](0, msg.column_formats(0)?)
      h.assert_eq[U8](0, msg.column_formats(1)?)
      h.assert_eq[U8](0, msg.column_formats(2)?)
    else
      h.fail("Wrong message type returned.")
    end

    // Binary format (1) with 1 column (binary format)
    let col_fmts2: Array[U8] val = recover val [as U8: 1] end
    let bytes2 = _IncomingCopyInResponseTestMessage(1, col_fmts2).bytes()
    let r2: Reader = Reader.>append(bytes2)

    match _ResponseParser(r2)?
    | let msg: _CopyInResponseMessage =>
      h.assert_eq[U8](1, msg.format)
      h.assert_eq[USize](1, msg.column_formats.size())
      h.assert_eq[U8](1, msg.column_formats(0)?)
    else
      h.fail("Wrong message type for binary format case.")
    end

class \nodoc\ iso _TestResponseParserCopyOutResponseMessage is UnitTest
  """
  Verify that CopyOutResponse ('H') messages are parsed into
  _CopyOutResponseMessage with correct fields.
  """
  fun name(): String =>
    "ResponseParser/CopyOutResponseMessage"

  fun apply(h: TestHelper) ? =>
    // Text format (0) with 2 columns (each text format)
    let col_fmts: Array[U8] val = recover val [as U8: 0; 0] end
    let bytes = _IncomingCopyOutResponseTestMessage(0, col_fmts).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: _CopyOutResponseMessage =>
      h.assert_eq[U8](0, msg.format)
      h.assert_eq[USize](2, msg.column_formats.size())
      h.assert_eq[U8](0, msg.column_formats(0)?)
      h.assert_eq[U8](0, msg.column_formats(1)?)
    else
      h.fail("Wrong message type returned.")
    end

    // Binary format (1) with 1 column (binary format)
    let col_fmts2: Array[U8] val = recover val [as U8: 1] end
    let bytes2 = _IncomingCopyOutResponseTestMessage(1, col_fmts2).bytes()
    let r2: Reader = Reader.>append(bytes2)

    match _ResponseParser(r2)?
    | let msg: _CopyOutResponseMessage =>
      h.assert_eq[U8](1, msg.format)
      h.assert_eq[USize](1, msg.column_formats.size())
      h.assert_eq[U8](1, msg.column_formats(0)?)
    else
      h.fail("Wrong message type for binary format case.")
    end

class \nodoc\ iso _TestResponseParserCopyDataMessage is UnitTest
  """
  Verify that CopyData ('d') messages from the backend are parsed into
  _CopyDataMessage with correct data payload.
  """
  fun name(): String =>
    "ResponseParser/CopyDataMessage"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "row1\tvalue1\n".array()
    let bytes = _IncomingCopyDataTestMessage(data).bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: _CopyDataMessage =>
      h.assert_array_eq[U8](data, msg.data)
    else
      h.fail("Wrong message type returned.")
    end

    // Empty data payload
    let empty: Array[U8] val = recover val Array[U8] end
    let bytes2 = _IncomingCopyDataTestMessage(empty).bytes()
    let r2: Reader = Reader.>append(bytes2)

    match _ResponseParser(r2)?
    | let msg: _CopyDataMessage =>
      h.assert_eq[USize](0, msg.data.size())
    else
      h.fail("Wrong message type for empty data case.")
    end

class \nodoc\ iso _TestResponseParserCopyDoneMessage is UnitTest
  """
  Verify that CopyDone ('c') messages from the backend are parsed into
  _CopyDoneMessage.
  """
  fun name(): String =>
    "ResponseParser/CopyDoneMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingCopyDoneTestMessage.bytes()
    let r: Reader = Reader.>append(bytes)

    if _ResponseParser(r)? isnt _CopyDoneMessage then
      h.fail("Wrong message type returned.")
    end

class \nodoc\ iso _TestResponseParserParameterStatusMessage is UnitTest
  """
  Verify that ParameterStatus ('S') messages are parsed into
  _ParameterStatusMessage with correct name and value fields.
  """
  fun name(): String =>
    "ResponseParser/ParameterStatusMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingParameterStatusTestMessage(
      "client_encoding", "UTF8").bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: _ParameterStatusMessage =>
      h.assert_eq[String]("client_encoding", msg.name)
      h.assert_eq[String]("UTF8", msg.value)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserNoticeResponseMessage is UnitTest
  """
  Verify that NoticeResponse ('N') messages are parsed into
  NoticeResponseMessage with correct severity, code, and message fields.
  """
  fun name(): String =>
    "ResponseParser/NoticeResponseMessage"

  fun apply(h: TestHelper) ? =>
    let bytes = _IncomingNoticeResponseTestMessage(
      "NOTICE", "00000", "test notice").bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: NoticeResponseMessage =>
      h.assert_eq[String]("NOTICE", msg.severity)
      h.assert_eq[String]("00000", msg.code)
      h.assert_eq[String]("test notice", msg.message)
    else
      h.fail("Wrong message returned.")
    end

class \nodoc\ iso _TestResponseParserNotificationResponseMessage is UnitTest
  """
  Verify that NotificationResponse ('A') messages are parsed into
  _NotificationResponseMessage with correct fields.
  """
  fun name(): String =>
    "ResponseParser/NotificationResponseMessage"

  fun apply(h: TestHelper) ? =>
    // Normal case: non-empty channel and payload
    let bytes = _IncomingNotificationResponseTestMessage(
      12345, "test_channel", "test_payload").bytes()
    let r: Reader = Reader.>append(bytes)

    match _ResponseParser(r)?
    | let msg: _NotificationResponseMessage =>
      h.assert_eq[I32](12345, msg.process_id)
      h.assert_eq[String]("test_channel", msg.channel)
      h.assert_eq[String]("test_payload", msg.payload)
    else
      h.fail("Wrong message type returned.")
    end

    // Edge case: empty payload
    let bytes2 = _IncomingNotificationResponseTestMessage(
      42, "ch", "").bytes()
    let r2: Reader = Reader.>append(bytes2)

    match _ResponseParser(r2)?
    | let msg: _NotificationResponseMessage =>
      h.assert_eq[I32](42, msg.process_id)
      h.assert_eq[String]("ch", msg.channel)
      h.assert_eq[String]("", msg.payload)
    else
      h.fail("Wrong message type for empty payload case.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesAsyncThenAuth is UnitTest
  """
  Verify correct buffer advancement across async message types (one parsed
  parameter status, one parsed notice, one parsed notification) followed by a
  known message (AuthenticationOk).
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/AsyncThenAuth"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingParameterStatusTestMessage(
      "server_version", "14.5").bytes())
    r.append(_IncomingNoticeResponseTestMessage(
      "NOTICE", "00000", "test").bytes())
    r.append(_IncomingNotificationResponseTestMessage(
      42, "ch", "msg").bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let msg: _ParameterStatusMessage =>
      h.assert_eq[String]("server_version", msg.name)
      h.assert_eq[String]("14.5", msg.value)
    else
      h.fail("Wrong message for ParameterStatus.")
    end

    match _ResponseParser(r)?
    | let msg: NoticeResponseMessage =>
      h.assert_eq[String]("NOTICE", msg.severity)
      h.assert_eq[String]("00000", msg.code)
      h.assert_eq[String]("test", msg.message)
    else
      h.fail("Wrong message for NoticeResponse.")
    end

    match _ResponseParser(r)?
    | let msg: _NotificationResponseMessage =>
      h.assert_eq[I32](42, msg.process_id)
      h.assert_eq[String]("ch", msg.channel)
      h.assert_eq[String]("msg", msg.payload)
    else
      h.fail("Wrong message for NotificationResponse.")
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message for AuthenticationOk.")
    end

class \nodoc\ val _IncomingParameterStatusTestMessage
  let _bytes: Array[U8] val

  new val create(name: String, value: String) =>
    let payload_size = 4 + name.size() + 1 + value.size() + 1
    let wb: Writer = Writer
    wb.u8(_MessageType.parameter_status())
    wb.u32_be(payload_size.u32())
    wb.write(name)
    wb.u8(0)
    wb.write(value)
    wb.u8(0)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingNoticeResponseTestMessage
  let _bytes: Array[U8] val

  new val create(severity: String, code: String, message: String) =>
    let payload_size = 4 +
      1 + severity.size() + 1 +
      1 + code.size() + 1 +
      1 + message.size() + 1 +
      1

    let wb: Writer = Writer
    wb.u8(_MessageType.notice_response())
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

class \nodoc\ val _IncomingNotificationResponseTestMessage
  let _bytes: Array[U8] val

  new val create(pid: I32, channel: String, payload: String) =>
    let payload_size = 4 + 4 + channel.size() + 1 + payload.size() + 1
    let wb: Writer = Writer
    wb.u8(_MessageType.notification_response())
    wb.u32_be(payload_size.u32())
    wb.i32_be(pid)
    wb.write(channel)
    wb.u8(0)
    wb.write(payload)
    wb.u8(0)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCopyInResponseTestMessage
  let _bytes: Array[U8] val

  new val create(format: U8, column_formats: Array[U8] val) =>
    // Payload: 4 (length) + 1 (format) + 2 (num columns) +
    //   (num_columns * 2) (column format codes as Int16)
    let num_cols = column_formats.size()
    let payload_size = 4 + 1 + 2 + (num_cols * 2)
    let wb: Writer = Writer
    wb.u8(_MessageType.copy_in_response())
    wb.u32_be(payload_size.u32())
    wb.u8(format)
    wb.u16_be(num_cols.u16())
    for fmt in column_formats.values() do
      wb.u16_be(fmt.u16())
    end

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCopyOutResponseTestMessage
  let _bytes: Array[U8] val

  new val create(format: U8, column_formats: Array[U8] val) =>
    let num_cols = column_formats.size()
    let payload_size = 4 + 1 + 2 + (num_cols * 2)
    let wb: Writer = Writer
    wb.u8(_MessageType.copy_out_response())
    wb.u32_be(payload_size.u32())
    wb.u8(format)
    wb.u16_be(num_cols.u16())
    for fmt in column_formats.values() do
      wb.u16_be(fmt.u16())
    end

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCopyDataTestMessage
  let _bytes: Array[U8] val

  new val create(data: Array[U8] val) =>
    let payload_size = 4 + data.size()
    let wb: Writer = Writer
    wb.u8(_MessageType.copy_data())
    wb.u32_be(payload_size.u32())
    wb.write(data)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

class \nodoc\ val _IncomingCopyDoneTestMessage
  let _bytes: Array[U8] val

  new val create() =>
    let wb: Writer = Writer
    wb.u8(_MessageType.copy_done())
    wb.u32_be(4)

    _bytes = WriterToByteArray(wb)

  fun bytes(): Array[U8] val =>
    _bytes

primitive \nodoc\ _RandomMessageBytesGen
  fun apply(rnd: Randomness, min_count: USize = 2,
    max_count: USize = 8): Array[Array[U8] val] val
  =>
    let count = rnd.usize(min_count, max_count)
    let messages = recover iso Array[Array[U8] val](count) end
    for _ in Range(0, count) do
      messages.push(_random_message(rnd))
    end
    consume messages

  fun _random_message(rnd: Randomness): Array[U8] val =>
    match rnd.usize(0, 25)
    | 0 => _IncomingAuthenticationOkTestMessage.bytes()
    | 1 =>
      _IncomingAuthenticationMD5PasswordTestMessage(
        _random_salt(rnd)).bytes()
    | 2 =>
      let mechanisms: Array[String] val = recover val
        ["SCRAM-SHA-256"]
      end
      _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
    | 3 =>
      _IncomingAuthenticationSASLContinueTestMessage(
        _random_bytes(rnd)).bytes()
    | 4 =>
      _IncomingAuthenticationSASLFinalTestMessage(
        _random_bytes(rnd)).bytes()
    | 5 =>
      _IncomingUnsupportedAuthenticationTestMessage(
        _random_unsupported_auth_type(rnd)).bytes()
    | 6 =>
      _IncomingBackendKeyDataTestMessage(rnd.i32(), rnd.i32()).bytes()
    | 7 =>
      _IncomingCommandCompleteTestMessage(
        _random_command_tag(rnd)).bytes()
    | 8 =>
      _IncomingCopyInResponseTestMessage(
        _random_copy_format(rnd), _random_column_formats(rnd)).bytes()
    | 9 =>
      _IncomingCopyOutResponseTestMessage(
        _random_copy_format(rnd), _random_column_formats(rnd)).bytes()
    | 10 =>
      _IncomingCopyDataTestMessage(_random_bytes(rnd)).bytes()
    | 11 => _IncomingCopyDoneTestMessage.bytes()
    | 12 =>
      _IncomingDataRowTestMessage(
        _random_data_row_columns(rnd)).bytes()
    | 13 => _IncomingEmptyQueryResponseTestMessage.bytes()
    | 14 =>
      _IncomingErrorResponseTestMessage(
        _safe_string(rnd), _safe_string(rnd), _safe_string(rnd)).bytes()
    | 15 =>
      _IncomingNoticeResponseTestMessage(
        _safe_string(rnd), _safe_string(rnd), _safe_string(rnd)).bytes()
    | 16 =>
      _IncomingNotificationResponseTestMessage(
        rnd.i32(), _safe_string(rnd), _safe_string(rnd)).bytes()
    | 17 =>
      _IncomingParameterStatusTestMessage(
        _safe_string(rnd), _safe_string(rnd)).bytes()
    | 18 =>
      _IncomingReadyForQueryTestMessage(_random_rfq_status(rnd)).bytes()
    | 19 =>
      try
        _IncomingRowDescriptionTestMessage(
          _random_row_desc_columns(rnd))?.bytes()
      else
        _IncomingAuthenticationOkTestMessage.bytes()
      end
    | 20 => _IncomingParseCompleteTestMessage.bytes()
    | 21 => _IncomingBindCompleteTestMessage.bytes()
    | 22 => _IncomingNoDataTestMessage.bytes()
    | 23 => _IncomingCloseCompleteTestMessage.bytes()
    | 24 =>
      _IncomingParameterDescriptionTestMessage(
        _random_oids(rnd)).bytes()
    | 25 => _IncomingPortalSuspendedTestMessage.bytes()
    else
      _IncomingAuthenticationOkTestMessage.bytes()
    end

  fun _safe_string(rnd: Randomness): String =>
    let size = rnd.usize(1, 20)
    recover val
      let s = String(size)
      for _ in Range(0, size) do
        s.push(rnd.u8(32, 126))
      end
      s
    end

  fun _random_salt(rnd: Randomness): String =>
    recover val
      let s = String(4)
      for _ in Range(0, 4) do
        s.push(rnd.u8(32, 126))
      end
      s
    end

  fun _random_bytes(rnd: Randomness): Array[U8] val =>
    let size = rnd.usize(1, 50)
    recover val
      let arr = Array[U8](size)
      for _ in Range(0, size) do
        arr.push(rnd.u8())
      end
      arr
    end

  fun _random_rfq_status(rnd: Randomness): U8 =>
    match rnd.usize(0, 2)
    | 0 => 'I'
    | 1 => 'T'
    else
      'E'
    end

  fun _random_command_tag(rnd: Randomness): String =>
    match rnd.usize(0, 6)
    | 0 => "SELECT " + rnd.usize(0, 100).string()
    | 1 => "INSERT 0 " + rnd.usize(0, 100).string()
    | 2 => "DELETE " + rnd.usize(0, 100).string()
    | 3 => "UPDATE " + rnd.usize(0, 100).string()
    | 4 => "CREATE TABLE"
    | 5 => "DROP TABLE"
    else
      "COPY " + rnd.usize(0, 100).string()
    end

  fun _random_unsupported_auth_type(rnd: Randomness): I32 =>
    match rnd.usize(0, 3)
    | 0 => 2
    | 1 => 3
    | 2 => 6
    else
      7
    end

  fun _random_oids(rnd: Randomness): Array[U32] val =>
    let size = rnd.usize(0, 5)
    recover val
      let arr = Array[U32](size)
      for _ in Range(0, size) do
        arr.push(rnd.u32())
      end
      arr
    end

  fun _random_copy_format(rnd: Randomness): U8 =>
    if rnd.bool() then 1 else 0 end

  fun _random_column_formats(rnd: Randomness): Array[U8] val =>
    let size = rnd.usize(0, 5)
    recover val
      let arr = Array[U8](size)
      for _ in Range(0, size) do
        arr.push(if rnd.bool() then 1 else 0 end)
      end
      arr
    end

  fun _random_data_row_columns(rnd: Randomness): Array[(String | None)] val =>
    let size = rnd.usize(0, 4)
    let arr = recover iso Array[(String | None)](size) end
    for _ in Range(0, size) do
      if rnd.bool() then
        arr.push(None)
      else
        arr.push(_safe_string(rnd))
      end
    end
    consume arr

  fun _random_row_desc_columns(rnd: Randomness):
    Array[(String, String)] val
  =>
    let known_types: Array[String] val = recover val
      ["text"; "int4"; "int8"; "bool"; "int2"; "float4"; "float8"; "bytea"]
    end
    let size = rnd.usize(1, 5)
    let arr = recover iso Array[(String, String)](size) end
    for i in Range(0, size) do
      try
        let type_idx = rnd.usize(0, known_types.size() - 1)
        arr.push(("col" + i.string(), known_types(type_idx)?))
      end
    end
    consume arr

class \nodoc\ iso _TestResponseParserMultipleMessagesChainProperty
  is Property1[Array[Array[U8] val] val]
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/Property"

  fun gen(): Generator[Array[Array[U8] val] val] =>
    Generator[Array[Array[U8] val] val](
      object is GenObj[Array[Array[U8] val] val]
        fun generate(rnd: Randomness): Array[Array[U8] val] val =>
          _RandomMessageBytesGen(rnd)
      end)

  fun ref property(arg1: Array[Array[U8] val] val, h: PropertyHelper) ? =>
    let r: Reader = Reader
    for msg in arg1.values() do
      r.append(msg)
    end

    for i in Range(0, arg1.size()) do
      if _ResponseParser(r)? is None then
        h.fail("Expected message at position " + i.string()
          + " but got None")
        return
      end
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Expected None after all messages consumed")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainSimpleQueryResult
  is UnitTest
  """
  Verify correct buffer advancement across a complete simple query response:
  RowDescription + DataRow + DataRow + CommandComplete + ReadyForQuery.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/SimpleQueryResult"

  fun apply(h: TestHelper) ? =>
    let columns: Array[(String, String)] val = recover val
      [("id", "int4"); ("name", "text")]
    end
    let r: Reader = Reader
    r.append(_IncomingRowDescriptionTestMessage(columns)?.bytes())
    r.append(_IncomingDataRowTestMessage(
      recover val [as (String | None): "1"; "Alice"] end).bytes())
    r.append(_IncomingDataRowTestMessage(
      recover val [as (String | None): "2"; "Bob"] end).bytes())
    r.append(_IncomingCommandCompleteTestMessage("SELECT 2").bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    match _ResponseParser(r)?
    | let m: _RowDescriptionMessage =>
      h.assert_eq[USize](2, m.columns.size())
      h.assert_eq[String]("id", m.columns(0)?._1)
      h.assert_eq[U32](23, m.columns(0)?._2)
      h.assert_eq[String]("name", m.columns(1)?._1)
      h.assert_eq[U32](25, m.columns(1)?._2)
    else
      h.fail("Wrong message for RowDescription.")
      return
    end

    match _ResponseParser(r)?
    | let m: _DataRowMessage =>
      h.assert_eq[USize](2, m.columns.size())
      match m.columns(0)?
      | "1" => None
      else
        h.fail("Row 1 col 0 not parsed correctly.")
        return
      end
      match m.columns(1)?
      | "Alice" => None
      else
        h.fail("Row 1 col 1 not parsed correctly.")
        return
      end
    else
      h.fail("Wrong message for first DataRow.")
      return
    end

    match _ResponseParser(r)?
    | let m: _DataRowMessage =>
      h.assert_eq[USize](2, m.columns.size())
      match m.columns(0)?
      | "2" => None
      else
        h.fail("Row 2 col 0 not parsed correctly.")
        return
      end
      match m.columns(1)?
      | "Bob" => None
      else
        h.fail("Row 2 col 1 not parsed correctly.")
        return
      end
    else
      h.fail("Wrong message for second DataRow.")
      return
    end

    match _ResponseParser(r)?
    | let m: _CommandCompleteMessage =>
      h.assert_eq[String]("SELECT", m.id)
      h.assert_eq[USize](2, m.value)
    else
      h.fail("Wrong message for CommandComplete.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainCopyOutSequence
  is UnitTest
  """
  Verify correct buffer advancement across a COPY TO STDOUT sequence:
  CopyOutResponse + CopyData + CopyData + CopyDone + CommandComplete +
  ReadyForQuery.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/CopyOutSequence"

  fun apply(h: TestHelper) ? =>
    let col_fmts: Array[U8] val = recover val [as U8: 0; 0] end
    let data1: Array[U8] val = "row1\tval1\n".array()
    let data2: Array[U8] val = "row2\tval2\n".array()
    let r: Reader = Reader
    r.append(_IncomingCopyOutResponseTestMessage(0, col_fmts).bytes())
    r.append(_IncomingCopyDataTestMessage(data1).bytes())
    r.append(_IncomingCopyDataTestMessage(data2).bytes())
    r.append(_IncomingCopyDoneTestMessage.bytes())
    r.append(_IncomingCommandCompleteTestMessage("COPY 2").bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    match _ResponseParser(r)?
    | let m: _CopyOutResponseMessage =>
      h.assert_eq[U8](0, m.format)
      h.assert_eq[USize](2, m.column_formats.size())
    else
      h.fail("Wrong message for CopyOutResponse.")
      return
    end

    match _ResponseParser(r)?
    | let m: _CopyDataMessage =>
      h.assert_array_eq[U8](data1, m.data)
    else
      h.fail("Wrong message for first CopyData.")
      return
    end

    match _ResponseParser(r)?
    | let m: _CopyDataMessage =>
      h.assert_array_eq[U8](data2, m.data)
    else
      h.fail("Wrong message for second CopyData.")
      return
    end

    if _ResponseParser(r)? isnt _CopyDoneMessage then
      h.fail("Wrong message for CopyDone.")
      return
    end

    match _ResponseParser(r)?
    | let m: _CommandCompleteMessage =>
      h.assert_eq[String]("COPY", m.id)
      h.assert_eq[USize](2, m.value)
    else
      h.fail("Wrong message for CommandComplete.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainEmptyQuerySequence
  is UnitTest
  """
  Verify correct buffer advancement across repeated empty query responses,
  testing ReadyForQuery as a non-final message in the chain.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/EmptyQuerySequence"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingEmptyQueryResponseTestMessage.bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())
    r.append(_IncomingEmptyQueryResponseTestMessage.bytes())
    r.append(_IncomingReadyForQueryTestMessage('T').bytes())

    if _ResponseParser(r)? isnt _EmptyQueryResponseMessage then
      h.fail("Wrong message for first EmptyQueryResponse.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for first ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt _EmptyQueryResponseMessage then
      h.fail("Wrong message for second EmptyQueryResponse.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionInBlock,
        m.transaction_status())
    else
      h.fail("Wrong message for second ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainPrepareSequence
  is UnitTest
  """
  Verify correct buffer advancement across a PREPARE response:
  ParseComplete + ParameterDescription + NoData + ReadyForQuery.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/PrepareSequence"

  fun apply(h: TestHelper) ? =>
    let oids: Array[U32] val = recover val [as U32: 23; 25] end
    let r: Reader = Reader
    r.append(_IncomingParseCompleteTestMessage.bytes())
    r.append(_IncomingParameterDescriptionTestMessage(oids).bytes())
    r.append(_IncomingNoDataTestMessage.bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    if _ResponseParser(r)? isnt _ParseCompleteMessage then
      h.fail("Wrong message for ParseComplete.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ParameterDescriptionMessage =>
      h.assert_eq[USize](2, m.param_oids.size())
      h.assert_eq[U32](23, m.param_oids(0)?)
      h.assert_eq[U32](25, m.param_oids(1)?)
    else
      h.fail("Wrong message for ParameterDescription.")
      return
    end

    if _ResponseParser(r)? isnt _NoDataMessage then
      h.fail("Wrong message for NoData.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso
  _TestResponseParserMultipleMessagesChainCloseStatementSequence
  is UnitTest
  """
  Verify correct buffer advancement across a close statement response:
  CloseComplete + ReadyForQuery.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/CloseStatementSequence"

  fun apply(h: TestHelper) ? =>
    let r: Reader = Reader
    r.append(_IncomingCloseCompleteTestMessage.bytes())
    r.append(_IncomingReadyForQueryTestMessage('I').bytes())

    if _ResponseParser(r)? isnt _CloseCompleteMessage then
      h.fail("Wrong message for CloseComplete.")
      return
    end

    match _ResponseParser(r)?
    | let m: _ReadyForQueryMessage =>
      h.assert_is[TransactionStatus](TransactionIdle,
        m.transaction_status())
    else
      h.fail("Wrong message for ReadyForQuery.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainSASLFullSequence
  is UnitTest
  """
  Verify correct buffer advancement across a SASL authentication sequence:
  AuthSASLContinue + AuthSASLFinal + AuthenticationOk.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/SASLFullSequence"

  fun apply(h: TestHelper) ? =>
    let continue_data: Array[U8] val = "r=abc123,s=c2FsdA==,i=4096".array()
    let final_data: Array[U8] val = "v=dGVzdA==".array()
    let r: Reader = Reader
    r.append(
      _IncomingAuthenticationSASLContinueTestMessage(continue_data).bytes())
    r.append(
      _IncomingAuthenticationSASLFinalTestMessage(final_data).bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLContinueMessage =>
      h.assert_array_eq[U8](continue_data, m.data)
    else
      h.fail("Wrong message for AuthSASLContinue.")
      return
    end

    match _ResponseParser(r)?
    | let m: _AuthenticationSASLFinalMessage =>
      h.assert_array_eq[U8](final_data, m.data)
    else
      h.fail("Wrong message for AuthSASLFinal.")
      return
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message for AuthenticationOk.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

class \nodoc\ iso _TestResponseParserMultipleMessagesChainRemainingTypes
  is UnitTest
  """
  Verify correct buffer advancement across the remaining message types not
  covered by other chain tests: CopyInResponse + UnsupportedAuth +
  PortalSuspended + AuthenticationOk.
  """
  fun name(): String =>
    "ResponseParser/MultipleMessages/Chain/RemainingTypes"

  fun apply(h: TestHelper) ? =>
    let col_fmts: Array[U8] val = recover val [as U8: 0] end
    let r: Reader = Reader
    r.append(_IncomingCopyInResponseTestMessage(0, col_fmts).bytes())
    r.append(_IncomingUnsupportedAuthenticationTestMessage(3).bytes())
    r.append(_IncomingPortalSuspendedTestMessage.bytes())
    r.append(_IncomingAuthenticationOkTestMessage.bytes())

    match _ResponseParser(r)?
    | let m: _CopyInResponseMessage =>
      h.assert_eq[U8](0, m.format)
      h.assert_eq[USize](1, m.column_formats.size())
      h.assert_eq[U8](0, m.column_formats(0)?)
    else
      h.fail("Wrong message for CopyInResponse.")
      return
    end

    if _ResponseParser(r)? isnt _UnsupportedAuthenticationMessage then
      h.fail("Wrong message for UnsupportedAuth.")
      return
    end

    if _ResponseParser(r)? isnt _PortalSuspendedMessage then
      h.fail("Wrong message for PortalSuspended.")
      return
    end

    if _ResponseParser(r)? isnt _AuthenticationOkMessage then
      h.fail("Wrong message for AuthenticationOk.")
      return
    end

    if _ResponseParser(r)? isnt None then
      h.fail("Buffer not fully consumed.")
    end

primitive WriterToByteArray
  fun apply(writer: Writer): Array[U8] val =>
    recover val
      let out = Array[U8]
      for b in writer.done().values() do
        out.append(b)
      end
      out
    end
