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
