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

