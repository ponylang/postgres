use "constrained_types"
use lori = "lori"
use "pony_test"

class \nodoc\ iso _TestConnectionTimeoutFires is UnitTest
  """
  Verifies that when a connection timeout is set and the server is unreachable,
  pg_session_connection_failed fires with ConnectionFailedTimeout.
  Connects to 192.0.2.1 (RFC 5737 TEST-NET-1, guaranteed non-routable).
  """
  fun name(): String =>
    "ConnectionTimeout/Fires"

  fun apply(h: TestHelper) =>
    match lori.MakeConnectionTimeout(100)
    | let ct: lori.ConnectionTimeout =>
      let session = Session(
        ServerConnectInfo(lori.TCPConnectAuth(h.env.root),
          "192.0.2.1", "9999"
          where connection_timeout' = ct),
        DatabaseConnectInfo("postgres", "postgres", "postgres"),
        _ConnectionTimeoutTestClient(h))
      h.dispose_when_done(session)
    | let _: ValidationFailure =>
      h.fail("Failed to create ConnectionTimeout.")
      h.complete(false)
    end
    h.long_test(5_000_000_000)

actor \nodoc\ _ConnectionTimeoutTestClient is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | ConnectionFailedTimeout =>
      _h.complete(true)
    else
      _h.fail("Expected ConnectionFailedTimeout but got different reason.")
      _h.complete(false)
    end

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have connected.")
    _h.complete(false)
