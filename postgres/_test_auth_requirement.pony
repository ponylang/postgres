use lori = "lori"
use "pony_test"

// AuthRequirement policy tests
//
// These tests drive mock servers that offer non-SCRAM authentication
// (AuthenticationOk, AuthenticationCleartextPassword,
// AuthenticationMD5Password) and verify that the default
// `AuthRequireSCRAM` policy rejects them with `AuthenticationMethodRejected`
// and — critically — that no password bytes reach the wire before rejection.
//
// Each test uses two expected actions:
//   - "client-rejected" — the notify saw
//     `pg_session_connection_failed(AuthenticationMethodRejected)` then
//     `pg_session_shutdown`.
//   - "no-password-leak" — the server saw the client's Terminate without
//     any preceding PasswordMessage. Without this action, a regression
//     that sent the password and *then* called the rejection callback
//     would still pass the callback contract.
//
// The counterpart — default `AuthRequireSCRAM` accepting SCRAM — is already
// covered by `_TestSCRAMAuthenticationSuccess` in `_test_auth.pony`, which
// constructs `ServerConnectInfo` without overriding `auth_requirement`.
// The "SCRAM-mechanism missing" path is covered by
// `_TestSCRAMUnsupportedMechanism`, which remains
// `UnsupportedAuthenticationMethod` (the existing, distinct variant).
//
// Acceptance tests for `AllowAnyAuth` (accepting AuthenticationOk, cleartext,
// and MD5) are covered by tests that already pass `AllowAnyAuth` explicitly —
// `_TestAuthenticate`, `_TestCleartextAuthenticationSuccess`, and the MD5
// integration tests in `_test_md5.pony`.

class \nodoc\ iso _TestAuthRequireSCRAMRejectsAuthenticationOk is UnitTest
  """
  Default `AuthRequireSCRAM` rejects a server that completes startup with
  `AuthenticationOk` (no authentication challenge) — the weakest posture.
  """
  fun name(): String =>
    "AuthRequirement/SCRAMRejectsAuthenticationOk"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7761"

    h.expect_action("client-rejected")
    h.expect_action("no-password-leak")

    let listener = _AuthMethodRejectedTestListener(
      lori.TCPListenAuth(h.env.root), host, port, h, _AuthReplyOk)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestAuthRequireSCRAMRejectsCleartextPassword is UnitTest
  """
  Default `AuthRequireSCRAM` rejects a server that offers
  `AuthenticationCleartextPassword`. The driver must not transmit the
  password in response.
  """
  fun name(): String =>
    "AuthRequirement/SCRAMRejectsCleartextPassword"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7762"

    h.expect_action("client-rejected")
    h.expect_action("no-password-leak")

    let listener = _AuthMethodRejectedTestListener(
      lori.TCPListenAuth(h.env.root), host, port, h, _AuthReplyCleartext)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestAuthRequireSCRAMRejectsMD5Password is UnitTest
  """
  Default `AuthRequireSCRAM` rejects a server that offers
  `AuthenticationMD5Password`. The driver must not transmit the
  MD5-hashed password in response.
  """
  fun name(): String =>
    "AuthRequirement/SCRAMRejectsMD5Password"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7763"

    h.expect_action("client-rejected")
    h.expect_action("no-password-leak")

    let listener = _AuthMethodRejectedTestListener(
      lori.TCPListenAuth(h.env.root), host, port, h, _AuthReplyMD5)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

primitive _AuthReplyOk
primitive _AuthReplyCleartext
primitive _AuthReplyMD5

type _AuthReplyKind is (_AuthReplyOk | _AuthReplyCleartext | _AuthReplyMD5)

actor \nodoc\ _AuthMethodRejectedTestListener is lori.TCPListenerActor
  """
  Listener that stands up a mock server responding with a single non-SCRAM
  authentication message selected by `_AuthReplyKind`. The client session
  uses the default `AuthRequirement` (`AuthRequireSCRAM`), so any of these
  replies must be rejected.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _kind: _AuthReplyKind

  new create(listen_auth: lori.TCPListenAuth, host: String, port: String,
    h: TestHelper, kind: _AuthReplyKind)
  =>
    _host = host
    _port = port
    _h = h
    _kind = kind
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _AuthMethodRejectedTestServer =>
    let server = _AuthMethodRejectedTestServer(_server_auth, fd, _h, _kind)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _AuthMethodRejectedTestNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _AuthMethodRejectedTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that replies to the client's startup message with a single
  non-SCRAM authentication message and otherwise does nothing.

  After sending the reply, the server watches every frontend message the
  client sends. A `PasswordMessage` ('p') fails the test — the client must
  never transmit credentials after an `AuthRequireSCRAM` rejection. A
  `Terminate` ('X') completes the "no-password-leak" action — the client
  finished cleanly without leaking credentials.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  let _kind: _AuthReplyKind
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    kind: _AuthReplyKind)
  =>
    _h = h
    _kind = kind
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if _state == 0 then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        let reply =
          match _kind
          | _AuthReplyOk =>
            _IncomingAuthenticationOkTestMessage.bytes()
          | _AuthReplyCleartext =>
            _IncomingAuthenticationCleartextPasswordTestMessage.bytes()
          | _AuthReplyMD5 =>
            _IncomingAuthenticationMD5PasswordTestMessage("salt").bytes()
          end
        _tcp_connection.send(reply)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let msg: Array[U8] val =>
        try
          let type_byte = msg(0)?
          if type_byte == 'p' then
            _h.fail(
              "Client sent PasswordMessage despite AuthenticationMethodRejected")
            _h.complete(false)
          elseif type_byte == 'X' then
            _h.complete_action("no-password-leak")
          end
        end
        _process()
      end
    end

actor \nodoc\ _AuthMethodRejectedTestNotify is SessionStatusNotify
  """
  Asserts that `pg_session_connection_failed` fires with
  `AuthenticationMethodRejected` and that `pg_session_shutdown` fires after
  it. Fails if authentication succeeds or if shutdown fires before the
  connection-failed callback.
  """
  let _h: TestHelper
  var _failed: Bool = false

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | AuthenticationMethodRejected =>
      _failed = true
    else
      _h.fail("Wrong failure reason.")
      _h.complete(false)
    end

  be pg_session_shutdown(session: Session) =>
    if not _failed then
      _h.fail("Shutdown fired before (or without) connection_failed.")
      _h.complete(false)
      return
    end
    _h.complete_action("client-rejected")
