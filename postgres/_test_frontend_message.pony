use "pony_test"

class \nodoc\ iso _TestFrontendMessagePassword is UnitTest
  fun name(): String =>
    "FrontendMessage/Password"

  fun apply(h: TestHelper) =>
    let password = "pwd"
    let expected: Array[U8] = ifdef bigendian then
      ['p'; 8; 0; 0; 0; 'p'; 'w'; 'd'; 0]
    else
      ['p'; 0; 0; 0; 8; 'p'; 'w'; 'd'; 0]
    end

    h.assert_array_eq[U8](expected, _FrontendMessage.password(password))

class \nodoc\ iso _TestFrontendMessageQuery is UnitTest
  fun name(): String =>
    "FrontendMessage/Query"

  fun apply(h: TestHelper) =>
    let query = "select * from free_candy"
    let expected: Array[U8] = ifdef bigendian then
      [ 81; 29; 0; 0; 0; 115; 101; 108; 101; 99; 116; 32; 42; 32; 102; 114
        111; 109; 32; 102; 114; 101; 101; 95; 99; 97; 110; 100; 121; 0 ]
    else
      [ 81; 0; 0; 0; 29; 115; 101; 108; 101; 99; 116; 32; 42; 32; 102; 114
        111; 109; 32; 102; 114; 101; 101; 95; 99; 97; 110; 100; 121; 0 ]
    end

    h.assert_array_eq[U8](expected, _FrontendMessage.query(query))

class \nodoc\ iso _TestFrontendMessageStartup is UnitTest
  fun name(): String =>
    "FrontendMessage/Startup"

  fun apply(h: TestHelper) =>
    let username = "pony"
    let password = "7669"
    let expected: Array[U8] = ifdef bigendian then
      [ 33; 0; 0; 0; 3; 0; 0; 0; 117; 115; 101; 114; 0; 112; 111; 110; 121; 0
        100; 97; 116; 97; 98; 97; 115; 101; 0; 55; 54; 54; 57; 0; 0 ]
    else
      [ 0; 0; 0; 33; 0; 3; 0; 0; 117; 115; 101; 114; 0; 112; 111; 110; 121; 0
        100; 97; 116; 97; 98; 97; 115; 101; 0; 55; 54; 54; 57; 0; 0 ]
    end

    h.assert_array_eq[U8](expected, _FrontendMessage.startup(username, password))

class \nodoc\ iso _TestFrontendMessageParse is UnitTest
  fun name(): String =>
    "FrontendMessage/Parse"

  fun apply(h: TestHelper) =>
    // Parse("", "S $1", [])
    // Length = 4 + 0+1 + 4+1 + 2 + 0 = 12, total = 13
    let expected: Array[U8] = ifdef bigendian then
      [ 80; 12; 0; 0; 0; 0; 83; 32; 36; 49; 0; 0; 0 ]
    else
      [ 80; 0; 0; 0; 12; 0; 83; 32; 36; 49; 0; 0; 0 ]
    end

    let oids: Array[U32] val = recover val Array[U32] end
    h.assert_array_eq[U8](expected,
      _FrontendMessage.parse("", "S $1", oids))

class \nodoc\ iso _TestFrontendMessageParseWithTypes is UnitTest
  fun name(): String =>
    "FrontendMessage/ParseWithTypes"

  fun apply(h: TestHelper) =>
    // Parse("", "S $1", [23])
    // Length = 4 + 0+1 + 4+1 + 2 + 4 = 16, total = 17
    let expected: Array[U8] = ifdef bigendian then
      [ 80; 16; 0; 0; 0; 0; 83; 32; 36; 49; 0; 1; 0; 23; 0; 0; 0 ]
    else
      [ 80; 0; 0; 0; 16; 0; 83; 32; 36; 49; 0; 0; 1; 0; 0; 0; 23 ]
    end

    let oids: Array[U32] val = recover val [as U32: 23] end
    h.assert_array_eq[U8](expected,
      _FrontendMessage.parse("", "S $1", oids))

class \nodoc\ iso _TestFrontendMessageBind is UnitTest
  fun name(): String =>
    "FrontendMessage/Bind"

  fun apply(h: TestHelper) =>
    // Bind("", "", ["abc"])
    // params_size = 4+3 = 7
    // Length = 4 + 0+1 + 0+1 + 2 + 2 + 7 + 2 = 19, total = 20
    let expected: Array[U8] = ifdef bigendian then
      [ 66; 19; 0; 0; 0; 0; 0; 0; 0; 1; 0
        3; 0; 0; 0; 97; 98; 99; 0; 0 ]
    else
      [ 66; 0; 0; 0; 19; 0; 0; 0; 0; 0; 1
        0; 0; 0; 3; 97; 98; 99; 0; 0 ]
    end

    let params: Array[(String | None)] val = recover val ["abc"] end
    h.assert_array_eq[U8](expected,
      _FrontendMessage.bind("", "", params))

class \nodoc\ iso _TestFrontendMessageBindWithNull is UnitTest
  fun name(): String =>
    "FrontendMessage/BindWithNull"

  fun apply(h: TestHelper) =>
    // Bind("", "", [None])
    // params_size = 4
    // Length = 4 + 0+1 + 0+1 + 2 + 2 + 4 + 2 = 16, total = 17
    let expected: Array[U8] = ifdef bigendian then
      [ 66; 16; 0; 0; 0; 0; 0; 0; 0; 1; 0
        255; 255; 255; 255; 0; 0 ]
    else
      [ 66; 0; 0; 0; 16; 0; 0; 0; 0; 0; 1
        255; 255; 255; 255; 0; 0 ]
    end

    let params: Array[(String | None)] val = recover val [None] end
    h.assert_array_eq[U8](expected,
      _FrontendMessage.bind("", "", params))

class \nodoc\ iso _TestFrontendMessageDescribePortal is UnitTest
  fun name(): String =>
    "FrontendMessage/DescribePortal"

  fun apply(h: TestHelper) =>
    // DescribePortal("")
    // Length = 4 + 1 + 0+1 = 6, total = 7
    let expected: Array[U8] = ifdef bigendian then
      [ 68; 6; 0; 0; 0; 80; 0 ]
    else
      [ 68; 0; 0; 0; 6; 80; 0 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.describe_portal(""))

class \nodoc\ iso _TestFrontendMessageExecute is UnitTest
  fun name(): String =>
    "FrontendMessage/Execute"

  fun apply(h: TestHelper) =>
    // ExecuteMsg("", 0)
    // Length = 4 + 0+1 + 4 = 9, total = 10
    let expected: Array[U8] = ifdef bigendian then
      [ 69; 9; 0; 0; 0; 0; 0; 0; 0; 0 ]
    else
      [ 69; 0; 0; 0; 9; 0; 0; 0; 0; 0 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.execute_msg("", 0))

class \nodoc\ iso _TestFrontendMessageDescribeStatement is UnitTest
  fun name(): String =>
    "FrontendMessage/DescribeStatement"

  fun apply(h: TestHelper) =>
    // DescribeStatement("s1")
    // Length = 4 + 1 + 2+1 = 8, total = 9
    let expected: Array[U8] = ifdef bigendian then
      [ 68; 8; 0; 0; 0; 83; 115; 49; 0 ]
    else
      [ 68; 0; 0; 0; 8; 83; 115; 49; 0 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.describe_statement("s1"))

class \nodoc\ iso _TestFrontendMessageCloseStatement is UnitTest
  fun name(): String =>
    "FrontendMessage/CloseStatement"

  fun apply(h: TestHelper) =>
    // CloseStatement("s1")
    // Length = 4 + 1 + 2+1 = 8, total = 9
    let expected: Array[U8] = ifdef bigendian then
      [ 67; 8; 0; 0; 0; 83; 115; 49; 0 ]
    else
      [ 67; 0; 0; 0; 8; 83; 115; 49; 0 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.close_statement("s1"))

class \nodoc\ iso _TestFrontendMessageSync is UnitTest
  fun name(): String =>
    "FrontendMessage/Sync"

  fun apply(h: TestHelper) =>
    // Sync()
    // Length = 4, total = 5
    let expected: Array[U8] = ifdef bigendian then
      [ 83; 4; 0; 0; 0 ]
    else
      [ 83; 0; 0; 0; 4 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.sync())

class \nodoc\ iso _TestFrontendMessageSSLRequest is UnitTest
  fun name(): String =>
    "FrontendMessage/SSLRequest"

  fun apply(h: TestHelper) =>
    // SSLRequest: Int32(8) Int32(80877103)
    // 80877103 = 0x04D2162F
    // Both big-endian and little-endian produce the same byte sequence
    // because the code writes big-endian wire format on both platforms.
    let expected: Array[U8] = [ 0; 0; 0; 8; 4; 210; 22; 47 ]

    h.assert_array_eq[U8](expected,
      _FrontendMessage.ssl_request())

class \nodoc\ iso _TestFrontendMessageCancelRequest is UnitTest
  fun name(): String =>
    "FrontendMessage/CancelRequest"

  fun apply(h: TestHelper) =>
    // CancelRequest: Int32(16) Int32(80877102) Int32(pid) Int32(key)
    // No message type byte, 16 bytes total.
    // 80877102 = 0x04D2162E
    // pid = 12345 = 0x00003039
    // key = 67890 = 0x00010932
    let expected: Array[U8] =
      [ 0; 0; 0; 16; 4; 210; 22; 46; 0; 0; 48; 57; 0; 1; 9; 50 ]

    h.assert_array_eq[U8](expected,
      _FrontendMessage.cancel_request(12345, 67890))

class \nodoc\ iso _TestFrontendMessageSASLInitialResponse is UnitTest
  fun name(): String =>
    "FrontendMessage/SASLInitialResponse"

  fun apply(h: TestHelper) =>
    // SASLInitialResponse("SCRAM-SHA-256", "n,,n=,r=abc")
    // mechanism = "SCRAM-SHA-256" (13 bytes + null)
    // response = "n,,n=,r=abc" (11 bytes)
    // Length = 4 + 13+1 + 4 + 11 = 33, total = 34
    let response: Array[U8] val = "n,,n=,r=abc".array()
    let expected: Array[U8] = ifdef bigendian then
      [ 'p'; 33; 0; 0; 0
        83; 67; 82; 65; 77; 45; 83; 72; 65; 45; 50; 53; 54; 0  // SCRAM-SHA-256\0
        11; 0; 0; 0  // response length
        110; 44; 44; 110; 61; 44; 114; 61; 97; 98; 99 ]  // n,,n=,r=abc
    else
      [ 'p'; 0; 0; 0; 33
        83; 67; 82; 65; 77; 45; 83; 72; 65; 45; 50; 53; 54; 0
        0; 0; 0; 11
        110; 44; 44; 110; 61; 44; 114; 61; 97; 98; 99 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.sasl_initial_response("SCRAM-SHA-256", response))

class \nodoc\ iso _TestFrontendMessageSASLResponse is UnitTest
  fun name(): String =>
    "FrontendMessage/SASLResponse"

  fun apply(h: TestHelper) =>
    // SASLResponse("c=biws,r=abc,p=proof")
    // response = 20 bytes
    // Length = 4 + 20 = 24, total = 25
    let response: Array[U8] val = "c=biws,r=abc,p=proof".array()
    let expected: Array[U8] = ifdef bigendian then
      [ 'p'; 24; 0; 0; 0
        99; 61; 98; 105; 119; 115; 44; 114; 61; 97; 98; 99; 44; 112; 61
        112; 114; 111; 111; 102 ]
    else
      [ 'p'; 0; 0; 0; 24
        99; 61; 98; 105; 119; 115; 44; 114; 61; 97; 98; 99; 44; 112; 61
        112; 114; 111; 111; 102 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.sasl_response(response))

class \nodoc\ iso _TestFrontendMessageTerminate is UnitTest
  fun name(): String =>
    "FrontendMessage/Terminate"

  fun apply(h: TestHelper) =>
    // Terminate: Byte1('X') Int32(4) = 5 bytes
    let expected: Array[U8] = ifdef bigendian then
      [ 'X'; 4; 0; 0; 0 ]
    else
      [ 'X'; 0; 0; 0; 4 ]
    end

    h.assert_array_eq[U8](expected,
      _FrontendMessage.terminate())
