use "encode/base64"
use lori = "lori"
use "pony_test"

// SCRAM-SHA-256 authentication unit tests

class \nodoc\ iso _TestSCRAMAuthenticationSuccess is UnitTest
  """
  Verifies the full SCRAM-SHA-256 authentication handshake: AuthSASL →
  SASLInitialResponse → SASLContinue → SASLResponse → SASLFinal → AuthOk →
  ReadyForQuery → pg_session_authenticated.
  """
  fun name(): String =>
    "SCRAM/AuthenticationSuccess"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7677"

    let listener = _SCRAMTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      false)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMUnsupportedMechanism is UnitTest
  """
  Verifies that when the server offers only unsupported SASL mechanisms,
  the session fires pg_session_authentication_failed with
  UnsupportedAuthenticationMethod.
  """
  fun name(): String =>
    "SCRAM/UnsupportedMechanism"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7678"

    let listener = _SCRAMUnsupportedTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMServerVerificationFailed is UnitTest
  """
  Verifies that when the server sends an incorrect signature in SASLFinal,
  the session fires pg_session_authentication_failed with
  ServerVerificationFailed.
  """
  fun name(): String =>
    "SCRAM/ServerVerificationFailed"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7679"

    let listener = _SCRAMTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      true)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMErrorDuringAuth is UnitTest
  """
  Verifies that when the server sends an ErrorResponse with code 28P01
  during SCRAM authentication, the session fires
  pg_session_authentication_failed with InvalidPassword.
  """
  fun name(): String =>
    "SCRAM/ErrorDuringAuth"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7681"

    let listener = _SCRAMErrorTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestUnsupportedAuthentication is UnitTest
  """
  Verifies that when the server requests an unsupported authentication type
  (e.g., cleartext password), the session fires pg_session_authentication_failed
  with UnsupportedAuthenticationMethod.
  """
  fun name(): String =>
    "UnsupportedAuthentication"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7682"

    let listener = _UnsupportedAuthenticationTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _UnsupportedAuthenticationTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _UnsupportedAuthenticationTestServer =>
    _UnsupportedAuthenticationTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, UnsupportedAuthenticationMethod))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _UnsupportedAuthenticationTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that sends a cleartext password authentication request (type 3),
  which the driver does not support.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _received then
      _received = true
      let msg = _IncomingUnsupportedAuthenticationTestMessage(3).bytes()
      _tcp_connection.send(msg)
    end

actor \nodoc\ _SCRAMSuccessTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.close()
    _h.complete(true)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication should have succeeded.")
    _h.complete(false)

actor \nodoc\ _SCRAMFailureTestNotify is SessionStatusNotify
  let _h: TestHelper
  let _expected_reason: AuthenticationFailureReason

  new create(h: TestHelper, expected: AuthenticationFailureReason) =>
    _h = h
    _expected_reason = expected

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_connection_failed(s: Session) =>
    _h.fail("Unable to establish connection.")
    _h.complete(false)

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    if reason is _expected_reason then
      _h.complete(true)
    else
      _h.fail("Wrong failure reason.")
      _h.complete(false)
    end

actor \nodoc\ _SCRAMTestListener is lori.TCPListenerActor
  """
  Listener for SCRAM-SHA-256 authentication tests. Creates a mock SCRAM
  server and connects a client session. Used by both the success test and
  the server verification failure test.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _send_wrong_signature: Bool

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    send_wrong_signature: Bool)
  =>
    _host = host
    _port = port
    _h = h
    _send_wrong_signature = send_wrong_signature
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SCRAMTestServer =>
    _SCRAMTestServer(_server_auth, fd, _h, _send_wrong_signature)

  fun ref _on_listening() =>
    let notify: SessionStatusNotify = if _send_wrong_signature then
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed)
    else
      _SCRAMSuccessTestNotify(_h)
    end
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      notify)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that performs a SCRAM-SHA-256 authentication handshake.
  Optionally sends a wrong server signature to test verification failure.

  The SCRAM exchange uses a fixed server nonce suffix, salt, and iteration
  count. The expected client proof and server signature are computed using
  _ScramSha256 with the test password "postgres".
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  let _send_wrong_signature: Bool
  var _received_count: USize = 0
  var _server_signature: (Array[U8] val | None) = None

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    send_wrong_signature: Bool)
  =>
    _h = h
    _send_wrong_signature = send_wrong_signature
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    let data_val: Array[U8] val = consume data
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup message: send AuthSASL with ["SCRAM-SHA-256"]
      let mechanisms: Array[String] val = recover val ["SCRAM-SHA-256"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    elseif _received_count == 2 then
      // SASLInitialResponse: parse client nonce, compute SCRAM values,
      // send SASLContinue.
      //
      // Wire format: 'p' I32(len) "SCRAM-SHA-256\0" I32(resp_len) response
      //   response starts at offset 23 (1 type + 4 len + 14 mech\0 + 4 resp_len)
      //   response = "n,,n=,r=<nonce>"
      //   client_first_bare starts at offset 26 (23 + 3, skip "n,,")
      //   client_nonce starts at offset 31 (23 + 8, skip "n,,n=,r=")
      try
        let client_nonce = recover val
          let s = String(data_val.size() - 31)
          var i: USize = 31
          while i < data_val.size() do
            s.push(data_val(i)?)
            i = i + 1
          end
          s
        end

        let client_first_bare = recover val
          let s = String(data_val.size() - 26)
          var i: USize = 26
          while i < data_val.size() do
            s.push(data_val(i)?)
            i = i + 1
          end
          s
        end

        let server_nonce = "servernonce123456"
        let combined_nonce: String val = client_nonce + server_nonce
        let salt: Array[U8] val =
          [0x73; 0x61; 0x6C; 0x74; 0x30; 0x31; 0x32; 0x33]
        let salt_b64_iso = Base64.encode(salt)
        let salt_b64: String val = consume salt_b64_iso
        let iterations: U32 = 4096

        let server_first: String val =
          "r=" + combined_nonce + ",s=" + salt_b64 + ",i=4096"

        (let client_proof, let sig) =
          _ScramSha256.compute_proof("postgres", salt, iterations,
            client_first_bare, server_first, combined_nonce)?

        // client_proof is unused — we don't verify the client's proof in this
        // mock server; the test validates the client's behavior, not ours.
        client_proof.size()

        _server_signature = sig

        let sasl_continue = _IncomingAuthenticationSASLContinueTestMessage(
          server_first.array()).bytes()
        _tcp_connection.send(sasl_continue)
      else
        _h.fail("SCRAM server computation failed")
        _h.complete(false)
      end
    elseif _received_count == 3 then
      // SASLResponse: send SASLFinal + AuthOk + ReadyForQuery
      match _server_signature
      | let sig: Array[U8] val =>
        let final_sig = if _send_wrong_signature then
          recover val Array[U8].init(0, 32) end
        else
          sig
        end
        let sig_b64_iso = Base64.encode(final_sig)
        let sig_b64: String val = consume sig_b64_iso
        let server_final: String val = "v=" + sig_b64
        let sasl_final = _IncomingAuthenticationSASLFinalTestMessage(
          server_final.array()).bytes()
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        let combined = recover val
          let arr = Array[U8]
          arr.append(sasl_final)
          arr.append(auth_ok)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
      end
    end

actor \nodoc\ _SCRAMUnsupportedTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SCRAMUnsupportedTestServer =>
    _SCRAMUnsupportedTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, UnsupportedAuthenticationMethod))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMUnsupportedTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that sends AuthSASL with only unsupported mechanisms.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _authed: Bool = false

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    if not _authed then
      _authed = true
      let mechanisms: Array[String] val =
        recover val ["SCRAM-SHA-256-PLUS"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    end

actor \nodoc\ _SCRAMErrorTestListener is lori.TCPListenerActor
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper)
  =>
    _host = host
    _port = port
    _h = h
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _SCRAMErrorTestServer =>
    _SCRAMErrorTestServer(_server_auth, fd)

  fun ref _on_listening() =>
    Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, InvalidPassword))

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMErrorTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that starts a SCRAM exchange then sends an ErrorResponse
  with code 28P01 (invalid password) after receiving SASLInitialResponse.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _received_count: USize = 0

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _received_count = _received_count + 1

    if _received_count == 1 then
      // Startup: send AuthSASL with ["SCRAM-SHA-256"]
      let mechanisms: Array[String] val =
        recover val ["SCRAM-SHA-256"] end
      let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
      _tcp_connection.send(sasl)
    elseif _received_count == 2 then
      // SASLInitialResponse: send ErrorResponse 28P01
      let err = _IncomingErrorResponseTestMessage("FATAL", "28P01",
        "password authentication failed").bytes()
      _tcp_connection.send(err)
    end
