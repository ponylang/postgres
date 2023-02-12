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
