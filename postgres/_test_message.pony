use "pony_test"

class \nodoc\ iso _TestMessagePassword is UnitTest
  fun name(): String =>
    "Message/Password"

  fun apply(h: TestHelper) =>
    let password = "pwd"
    let expected: Array[U8] = ifdef bigendian then
      ['p'; 8; 0; 0; 0; 'p'; 'w'; 'd'; 0]
    else
      ['p'; 0; 0; 0; 8; 'p'; 'w'; 'd'; 0]
    end

    h.assert_array_eq[U8](expected, _Message.password(password))

class \nodoc\ iso _TestMessageStartup is UnitTest
  fun name(): String =>
    "Message/Startup"

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

    h.assert_array_eq[U8](expected, _Message.startup(username, password))
