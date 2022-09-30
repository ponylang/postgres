use "pony_test"

actor \nodoc\ Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)

  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_IntegrationTestToReplace)
    test(_UnitTestToReplace)

class iso _IntegrationTestToReplace is UnitTest
  fun name(): String =>
    "integration/TestToReplace"

  fun apply(h: TestHelper) =>
    h.assert_true(true)

class iso _UnitTestToReplace is UnitTest
  fun name(): String =>
    "TestToReplace"

  fun apply(h: TestHelper) =>
    h.assert_true(true)
