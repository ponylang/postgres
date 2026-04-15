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
  the session fires pg_session_connection_failed with
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
  the session fires pg_session_connection_failed with
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
  pg_session_connection_failed with InvalidPassword carrying SQLSTATE
  "28P01".
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

class \nodoc\ iso _TestSCRAMServerSkipsSASLFinal is UnitTest
  """
  Verifies that when the server sends AuthenticationOk without a preceding
  AuthenticationSASLFinal, the session fires pg_session_connection_failed
  with ServerVerificationFailed. A server must never be treated as
  authenticated without signature verification.
  """
  fun name(): String =>
    "SCRAM/ServerSkipsSASLFinal"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7725"

    let listener = _SCRAMSkipSASLFinalTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMDuplicateSASLContinue is UnitTest
  """
  Verifies that when the server sends a second AuthenticationSASLContinue
  after the first one has established the expected server signature, the
  session fires pg_session_connection_failed with ServerVerificationFailed.
  A duplicate SASLContinue would overwrite the verifier, resetting
  verification state.
  """
  fun name(): String =>
    "SCRAM/DuplicateSASLContinue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7726"

    let listener = _SCRAMDuplicateSASLContinueTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMSASLFinalBeforeSASLContinue is UnitTest
  """
  Verifies that when the server sends AuthenticationSASLFinal before any
  AuthenticationSASLContinue, the session fires
  pg_session_connection_failed with ServerVerificationFailed. No server
  signature has been computed, so the SASLFinal cannot be verified.
  """
  fun name(): String =>
    "SCRAM/SASLFinalBeforeSASLContinue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7727"

    let listener = _SCRAMSASLFinalBeforeContinueTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMMalformedSASLFinal is UnitTest
  """
  Verifies that when the server sends an AuthenticationSASLFinal whose
  payload does not begin with the "v=" verifier prefix, the session fires
  pg_session_connection_failed with ServerVerificationFailed. Per RFC 5802,
  the only SASLFinal form PostgreSQL uses is "v=<verifier>"; anything else
  is a protocol violation.
  """
  fun name(): String =>
    "SCRAM/MalformedSASLFinal"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7728"

    let listener = _SCRAMMalformedSASLFinalTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMNonceMismatch is UnitTest
  """
  Verifies that when the server's AuthenticationSASLContinue carries a
  combined nonce that does not begin with the client's nonce, the session
  fires pg_session_connection_failed with ServerVerificationFailed. A
  combined nonce that replaces the client's contribution would let the
  server impersonate a different session.
  """
  fun name(): String =>
    "SCRAM/NonceMismatch"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7729"

    let listener = _SCRAMNonceMismatchTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestSCRAMMalformedSASLContinue is UnitTest
  """
  Verifies that when the server's AuthenticationSASLContinue cannot be
  parsed (e.g., non-numeric iteration count), the session fires
  pg_session_connection_failed with ServerVerificationFailed rather than
  silently closing.
  """
  fun name(): String =>
    "SCRAM/MalformedSASLContinue"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7730"

    let listener = _SCRAMMalformedSASLContinueTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestUnsupportedAuthentication is UnitTest
  """
  Verifies that when the server requests an unsupported authentication type
  (e.g., KerberosV5), the session fires pg_session_connection_failed
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
    let server = _UnsupportedAuthenticationTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, UnsupportedAuthenticationMethod))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _UnsupportedAuthenticationTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that sends a KerberosV5 authentication request (type 2),
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
      let msg = _IncomingUnsupportedAuthenticationTestMessage(2).bytes()
      _tcp_connection.send(msg)
    end

// Cleartext password authentication tests

class \nodoc\ iso _TestCleartextAuthenticationSuccess is UnitTest
  """
  Verifies the full cleartext password authentication handshake:
  AuthCleartextPassword -> PasswordMessage -> AuthOk -> ReadyForQuery ->
  pg_session_authenticated.
  """
  fun name(): String =>
    "Cleartext/AuthenticationSuccess"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7722"

    let listener = _CleartextTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      false)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

class \nodoc\ iso _TestCleartextAuthenticationFailure is UnitTest
  """
  Verifies that when cleartext password authentication fails (wrong password),
  the session fires pg_session_connection_failed with InvalidPassword
  carrying SQLSTATE "28P01".
  """
  fun name(): String =>
    "Cleartext/AuthenticationFailure"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7723"

    let listener = _CleartextTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h,
      true)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _CleartextTestListener is lori.TCPListenerActor
  """
  Listener for cleartext password authentication tests. Creates a mock
  cleartext server and connects a client session.
  """
  var _tcp_listener: lori.TCPListener = lori.TCPListener.none()
  let _server_auth: lori.TCPServerAuth
  let _h: TestHelper
  let _host: String
  let _port: String
  let _send_error: Bool

  new create(listen_auth: lori.TCPListenAuth,
    host: String,
    port: String,
    h: TestHelper,
    send_error: Bool)
  =>
    _host = host
    _port = port
    _h = h
    _send_error = send_error
    _server_auth = lori.TCPServerAuth(listen_auth)
    _tcp_listener = lori.TCPListener(listen_auth, host, port, this)

  fun ref _listener(): lori.TCPListener =>
    _tcp_listener

  fun ref _on_accept(fd: U32): _CleartextTestServer =>
    let server = _CleartextTestServer(_server_auth, fd, _send_error)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let notify: SessionStatusNotify = if _send_error then
      _SCRAMFailureTestNotify(_h, _ExpectedSqlstate("28P01"))
    else
      _SCRAMSuccessTestNotify(_h)
    end
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port
        where auth_requirement' = AllowAnyAuth),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      notify)
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _CleartextTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that performs a cleartext password authentication handshake.
  Receives a startup message, sends AuthenticationCleartextPassword, receives
  the password, then sends AuthenticationOk + ReadyForQuery (or ErrorResponse
  28P01 if _send_error is true).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _send_error: Bool
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, send_error: Bool) =>
    _send_error = send_error
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
        let msg =
          _IncomingAuthenticationCleartextPasswordTestMessage.bytes()
        _tcp_connection.send(msg)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        if _send_error then
          let err = _IncomingErrorResponseTestMessage(
            "FATAL", "28P01", "password authentication failed").bytes()
          _tcp_connection.send(err)
        else
          let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
          let ready = _IncomingReadyForQueryTestMessage('I').bytes()
          let combined = recover val
            let arr = Array[U8]
            arr.append(auth_ok)
            arr.append(ready)
            arr
          end
          _tcp_connection.send(combined)
        end
      end
    end

actor \nodoc\ _SCRAMSuccessTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.close()
    _h.complete(true)

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    _h.fail("Connection should have succeeded.")
    _h.complete(false)

class \nodoc\ val _ExpectedSqlstate
  """
  Wrapper for an expected SQLSTATE code used by `_SCRAMFailureTestNotify`.
  Distinct type — not a bare `String` — so the match arms that pair
  expected-values with `ConnectionFailureReason` variants cannot collide
  with any future `String`-valued variant of `ConnectionFailureReason`.
  """
  let code: String
  new val create(code': String) =>
    code = code'

actor \nodoc\ _SCRAMFailureTestNotify is SessionStatusNotify
  """
  Asserts that `pg_session_connection_failed` fires with an expected reason.
  Primitive variants (e.g., `UnsupportedAuthenticationMethod`) are matched by
  identity. Class variants that wrap an `ErrorResponseMessage` are identified
  by the SQLSTATE code they carry — pass `_ExpectedSqlstate(code)` instead
  of constructing a placeholder class instance.
  """
  let _h: TestHelper
  let _expected: (ConnectionFailureReason | _ExpectedSqlstate)

  new create(h: TestHelper,
    expected: (ConnectionFailureReason | _ExpectedSqlstate))
  =>
    _h = h
    _expected = expected

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    let ok =
      match (_expected, reason)
      | (UnsupportedAuthenticationMethod, UnsupportedAuthenticationMethod) =>
        true
      | (AuthenticationMethodRejected, AuthenticationMethodRejected) => true
      | (ServerVerificationFailed, ServerVerificationFailed) => true
      | (let e: _ExpectedSqlstate, let r: InvalidPassword) =>
        r.response().code == e.code
      | (let e: _ExpectedSqlstate, let r: InvalidAuthorizationSpecification) =>
        r.response().code == e.code
      | (let e: _ExpectedSqlstate, let r: TooManyConnections) =>
        r.response().code == e.code
      | (let e: _ExpectedSqlstate, let r: InvalidDatabaseName) =>
        r.response().code == e.code
      | (let e: _ExpectedSqlstate, let r: ServerRejected) =>
        r.response().code == e.code
      else
        false
      end
    if ok then
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
    let server = _SCRAMTestServer(_server_auth, fd, _h, _send_wrong_signature)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let notify: SessionStatusNotify = if _send_wrong_signature then
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed)
    else
      _SCRAMSuccessTestNotify(_h)
    end
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      notify)
    _h.dispose_when_done(session)

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
  var _state: U8 = 0
  var _server_signature: (Array[U8] val | None) = None
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper,
    send_wrong_signature: Bool)
  =>
    _h = h
    _send_wrong_signature = send_wrong_signature
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
        // Startup message: send AuthSASL with ["SCRAM-SHA-256"]
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let data_val: Array[U8] val =>
        match _SCRAMMockHelper.build(data_val)
        | let r: _SCRAMMockContinue =>
          _server_signature = r.server_signature
          _tcp_connection.send(r.continue_bytes)
          _state = 2
          _process()
        else
          _h.fail("SCRAM server computation failed")
          _h.complete(false)
        end
      end
    elseif _state == 2 then
      match _reader.read_message()
      | let _: Array[U8] val =>
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
    let server = _SCRAMUnsupportedTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, UnsupportedAuthenticationMethod))
    _h.dispose_when_done(session)

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
    let server = _SCRAMErrorTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, _ExpectedSqlstate("28P01")))
    _h.dispose_when_done(session)

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
  var _authed: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    _process()

  fun ref _process() =>
    if not _authed then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _authed = true
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _process()
      end
    else
      match _reader.read_message()
      | let _: Array[U8] val =>
        let err = _IncomingErrorResponseTestMessage("FATAL", "28P01",
          "password authentication failed").bytes()
        _tcp_connection.send(err)
      end
    end

actor \nodoc\ _SCRAMSkipSASLFinalTestListener is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMServerSkipsSASLFinal`. Spawns a mock server that
  skips SASLFinal in the SCRAM exchange and connects a client session.
  """
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

  fun ref _on_accept(fd: U32): _SCRAMSkipSASLFinalTestServer =>
    let server = _SCRAMSkipSASLFinalTestServer(_server_auth, fd, _h)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMSkipSASLFinalTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that runs a SCRAM-SHA-256 exchange through
  AuthenticationSASLContinue, receives the client's SASLResponse, then
  skips SASLFinal entirely and sends AuthenticationOk + ReadyForQuery.
  The client must reject this as a protocol violation.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let data_val: Array[U8] val =>
        match _SCRAMMockHelper.build(data_val)
        | let r: _SCRAMMockContinue =>
          _tcp_connection.send(r.continue_bytes)
          _state = 2
          _process()
        else
          _h.fail("SCRAM mock computation failed")
          _h.complete(false)
        end
      end
    elseif _state == 2 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        // Skip SASLFinal — send AuthOk + ReadyForQuery directly. The
        // client's `_server_verified` guard must reject this.
        let auth_ok = _IncomingAuthenticationOkTestMessage.bytes()
        let ready = _IncomingReadyForQueryTestMessage('I').bytes()
        let combined = recover val
          let arr = Array[U8]
          arr.append(auth_ok)
          arr.append(ready)
          arr
        end
        _tcp_connection.send(combined)
      end
    end

actor \nodoc\ _SCRAMDuplicateSASLContinueTestListener is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMDuplicateSASLContinue`. Spawns a mock server
  that sends two SASLContinue messages back-to-back.
  """
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

  fun ref _on_accept(fd: U32): _SCRAMDuplicateSASLContinueTestServer =>
    let server = _SCRAMDuplicateSASLContinueTestServer(_server_auth, fd, _h)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMDuplicateSASLContinueTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that answers the client's SASLInitialResponse with two
  AuthenticationSASLContinue messages sent in a single TCP write. The
  first is valid and lets the client populate its expected-verifier
  state; the second must be rejected by the duplicate-SASLContinue
  guard.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let data_val: Array[U8] val =>
        match _SCRAMMockHelper.build(data_val)
        | let r: _SCRAMMockContinue =>
          let combined = recover val
            let arr = Array[U8]
            arr.append(r.continue_bytes)
            arr.append(r.continue_bytes)
            arr
          end
          _tcp_connection.send(combined)
          _state = 2
        else
          _h.fail("SCRAM mock computation failed")
          _h.complete(false)
        end
      end
    end

actor \nodoc\ _SCRAMSASLFinalBeforeContinueTestListener
  is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMSASLFinalBeforeSASLContinue`. Spawns a mock
  server that sends SASLFinal without ever sending SASLContinue.
  """
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

  fun ref _on_accept(fd: U32): _SCRAMSASLFinalBeforeContinueTestServer =>
    let server =
      _SCRAMSASLFinalBeforeContinueTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMSASLFinalBeforeContinueTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that answers the client's SASLInitialResponse with a
  SASLFinal directly, without a preceding SASLContinue. The SASLFinal
  body starts with "v=" but the client has no expected signature to
  compare against.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        let server_final: String val = "v=c29tZWRhdGE="
        let sasl_final = _IncomingAuthenticationSASLFinalTestMessage(
          server_final.array()).bytes()
        _tcp_connection.send(sasl_final)
        _state = 2
      end
    end

actor \nodoc\ _SCRAMMalformedSASLFinalTestListener is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMMalformedSASLFinal`. Spawns a mock server that
  completes SCRAM up to SASLFinal, then sends a SASLFinal payload that
  does not begin with "v=".
  """
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

  fun ref _on_accept(fd: U32): _SCRAMMalformedSASLFinalTestServer =>
    let server = _SCRAMMalformedSASLFinalTestServer(_server_auth, fd, _h)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMMalformedSASLFinalTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that runs a SCRAM-SHA-256 exchange through
  AuthenticationSASLContinue, receives the client's SASLResponse, then
  sends an AuthenticationSASLFinal whose payload does not begin with
  "v=". The client must treat this as a protocol violation.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let data_val: Array[U8] val =>
        match _SCRAMMockHelper.build(data_val)
        | let r: _SCRAMMockContinue =>
          _tcp_connection.send(r.continue_bytes)
          _state = 2
          _process()
        else
          _h.fail("SCRAM mock computation failed")
          _h.complete(false)
        end
      end
    elseif _state == 2 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        let server_final: String val = "garbage-without-v-prefix"
        let sasl_final = _IncomingAuthenticationSASLFinalTestMessage(
          server_final.array()).bytes()
        _tcp_connection.send(sasl_final)
      end
    end

actor \nodoc\ _SCRAMNonceMismatchTestListener is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMNonceMismatch`. Spawns a mock server that sends
  a SASLContinue whose combined nonce does not include the client's
  nonce.
  """
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

  fun ref _on_accept(fd: U32): _SCRAMNonceMismatchTestServer =>
    let server = _SCRAMNonceMismatchTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMNonceMismatchTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server whose SASLContinue carries a combined nonce that does not
  begin with the client's nonce. The client's nonce-prefix check must
  reject as a protocol violation.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let _: Array[U8] val =>
        // `server_first` uses a hardcoded nonce that cannot match the
        // client's random nonce. Salt is valid base64 and iterations is
        // a valid integer, so parsing succeeds up to the nonce check.
        let server_first: String val = "r=badnonce,s=c2FsdA==,i=4096"
        let sasl_continue = _IncomingAuthenticationSASLContinueTestMessage(
          server_first.array()).bytes()
        _tcp_connection.send(sasl_continue)
        _state = 2
      end
    end

actor \nodoc\ _SCRAMMalformedSASLContinueTestListener
  is lori.TCPListenerActor
  """
  Listener for `_TestSCRAMMalformedSASLContinue`. Spawns a mock server
  that sends a SASLContinue whose iteration count is not a number.
  """
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

  fun ref _on_accept(fd: U32): _SCRAMMalformedSASLContinueTestServer =>
    let server = _SCRAMMalformedSASLContinueTestServer(_server_auth, fd, _h)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _SCRAMFailureTestNotify(_h, ServerVerificationFailed))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _SCRAMMalformedSASLContinueTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server whose SASLContinue uses the client's nonce (so the nonce
  prefix check passes) but has a non-numeric `i=` field. The client's
  outer `try`/`else` parse-failure path must fire
  `pg_session_connection_failed` with `ServerVerificationFailed`.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _h: TestHelper
  var _state: U8 = 0
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32, h: TestHelper) =>
    _h = h
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
        let mechanisms: Array[String] val =
          recover val ["SCRAM-SHA-256"] end
        let sasl = _IncomingAuthenticationSASLTestMessage(mechanisms).bytes()
        _tcp_connection.send(sasl)
        _state = 1
        _process()
      end
    elseif _state == 1 then
      match _reader.read_message()
      | let data_val: Array[U8] val =>
        match _SCRAMMockHelper.build(data_val)
        | let r: _SCRAMMockContinue =>
          let combined_nonce: String val =
            r.client_nonce + "servernonce123456"
          let server_first: String val =
            "r=" + combined_nonce + ",s=c2FsdA==,i=notanumber"
          let sasl_continue = _IncomingAuthenticationSASLContinueTestMessage(
            server_first.array()).bytes()
          _tcp_connection.send(sasl_continue)
          _state = 2
        else
          _h.fail("SCRAM mock computation failed")
          _h.complete(false)
        end
      end
    end

class \nodoc\ val _SCRAMMockContinue
  """
  Output of `_SCRAMMockHelper.build`: the SASLContinue bytes to send to the
  client, the matching server signature a subsequent SASLFinal would embed,
  and the client nonce extracted from the client's SASLInitialResponse.
  """
  let continue_bytes: Array[U8] val
  let server_signature: Array[U8] val
  let client_nonce: String val

  new val create(continue_bytes': Array[U8] val,
    server_signature': Array[U8] val,
    client_nonce': String val)
  =>
    continue_bytes = continue_bytes'
    server_signature = server_signature'
    client_nonce = client_nonce'

primitive \nodoc\ _SCRAMMockHelper
  """
  Computes a valid SCRAM-SHA-256 server-first-message for a received
  SASLInitialResponse, using a fixed server nonce suffix, salt, and
  iteration count against the test password "postgres". Returns the
  SASLContinue bytes, the matching server signature, and the extracted
  client nonce. Shared by the success/failure SCRAM mock servers so the
  SCRAM computation and wire-offset math live in one place.
  """
  fun build(data_val: Array[U8] val): (_SCRAMMockContinue | None) =>
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

      (let client_proof, let server_signature) =
        _ScramSha256.compute_proof("postgres", salt, iterations,
          client_first_bare, server_first, combined_nonce)?

      // client_proof is unused — the mock servers validate the client's
      // behavior, not their own proof computation.
      client_proof.size()

      let continue_bytes = _IncomingAuthenticationSASLContinueTestMessage(
        server_first.array()).bytes()

      _SCRAMMockContinue(continue_bytes, server_signature, client_nonce)
    end

primitive \nodoc\ _ConnectionFailureReasonFromErrorTestHelper
  fun err(code: String): ErrorResponseMessage =>
    ErrorResponseMessage("FATAL", None, code, "test message",
      None, None, None, None, None, None, None, None, None, None, None,
      None, None, None)

class \nodoc\ iso _TestConnectionFailureReasonFromErrorInvalidPassword
  is UnitTest
  """
  Verifies that _ConnectionFailureReasonFromError maps SQLSTATE 28P01 to
  InvalidPassword, preserving the full ErrorResponseMessage.
  """
  fun name(): String => "ConnectionFailureReasonFromError/InvalidPassword"

  fun apply(h: TestHelper) =>
    let msg = _ConnectionFailureReasonFromErrorTestHelper.err("28P01")
    match _ConnectionFailureReasonFromError(msg)
    | let r: InvalidPassword =>
      if r.response().code != "28P01" then
        h.fail("Wrong SQLSTATE: " + r.response().code)
      end
    else
      h.fail("Expected InvalidPassword for SQLSTATE 28P01.")
    end

class \nodoc\ iso
  _TestConnectionFailureReasonFromErrorInvalidAuthorizationSpecification
  is UnitTest
  """
  Verifies that _ConnectionFailureReasonFromError maps SQLSTATE 28000 to
  InvalidAuthorizationSpecification.
  """
  fun name(): String =>
    "ConnectionFailureReasonFromError/InvalidAuthorizationSpecification"

  fun apply(h: TestHelper) =>
    let msg = _ConnectionFailureReasonFromErrorTestHelper.err("28000")
    match _ConnectionFailureReasonFromError(msg)
    | let r: InvalidAuthorizationSpecification =>
      if r.response().code != "28000" then
        h.fail("Wrong SQLSTATE: " + r.response().code)
      end
    else
      h.fail("Expected InvalidAuthorizationSpecification for SQLSTATE 28000.")
    end

class \nodoc\ iso _TestConnectionFailureReasonFromErrorTooManyConnections
  is UnitTest
  """
  Verifies that _ConnectionFailureReasonFromError maps SQLSTATE 53300 to
  TooManyConnections.
  """
  fun name(): String =>
    "ConnectionFailureReasonFromError/TooManyConnections"

  fun apply(h: TestHelper) =>
    let msg = _ConnectionFailureReasonFromErrorTestHelper.err("53300")
    match _ConnectionFailureReasonFromError(msg)
    | let r: TooManyConnections =>
      if r.response().code != "53300" then
        h.fail("Wrong SQLSTATE: " + r.response().code)
      end
    else
      h.fail("Expected TooManyConnections for SQLSTATE 53300.")
    end

class \nodoc\ iso _TestConnectionFailureReasonFromErrorInvalidDatabaseName
  is UnitTest
  """
  Verifies that _ConnectionFailureReasonFromError maps SQLSTATE 3D000 to
  InvalidDatabaseName.
  """
  fun name(): String =>
    "ConnectionFailureReasonFromError/InvalidDatabaseName"

  fun apply(h: TestHelper) =>
    let msg = _ConnectionFailureReasonFromErrorTestHelper.err("3D000")
    match _ConnectionFailureReasonFromError(msg)
    | let r: InvalidDatabaseName =>
      if r.response().code != "3D000" then
        h.fail("Wrong SQLSTATE: " + r.response().code)
      end
    else
      h.fail("Expected InvalidDatabaseName for SQLSTATE 3D000.")
    end

class \nodoc\ iso _TestConnectionFailureReasonFromErrorServerRejected
  is UnitTest
  """
  Verifies that _ConnectionFailureReasonFromError falls back to
  ServerRejected for any SQLSTATE not explicitly mapped.
  """
  fun name(): String =>
    "ConnectionFailureReasonFromError/ServerRejected"

  fun apply(h: TestHelper) =>
    let msg = _ConnectionFailureReasonFromErrorTestHelper.err("XX000")
    match _ConnectionFailureReasonFromError(msg)
    | let r: ServerRejected =>
      if r.response().code != "XX000" then
        h.fail("Wrong SQLSTATE: " + r.response().code)
      end
    else
      h.fail("Expected ServerRejected for unmapped SQLSTATE XX000.")
    end

class \nodoc\ iso _TestConnectionFailedOnServerRejection is UnitTest
  """
  Regression test for issue #203. When the server rejects the startup with
  an ErrorResponse (e.g., SQLSTATE 53300 for `max_connections` exceeded)
  before any authentication request, the driver must deliver the failure
  via pg_session_connection_failed instead of crashing through an
  unreachable state.
  """
  fun name(): String =>
    "ConnectionFailedOnServerRejection"

  fun apply(h: TestHelper) =>
    let host = "127.0.0.1"
    let port = "7724"

    let listener = _TooManyConnectionsTestListener(
      lori.TCPListenAuth(h.env.root),
      host,
      port,
      h)

    h.dispose_when_done(listener)
    h.long_test(5_000_000_000)

actor \nodoc\ _TooManyConnectionsTestListener is lori.TCPListenerActor
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

  fun ref _on_accept(fd: U32): _TooManyConnectionsTestServer =>
    let server = _TooManyConnectionsTestServer(_server_auth, fd)
    _h.dispose_when_done(server)
    server

  fun ref _on_listening() =>
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _host, _port),
      DatabaseConnectInfo("postgres", "postgres", "postgres"),
      _TooManyConnectionsTestNotify(_h))
    _h.dispose_when_done(session)

  fun ref _on_listen_failure() =>
    _h.fail("Unable to listen")
    _h.complete(false)

actor \nodoc\ _TooManyConnectionsTestServer
  is (lori.TCPConnectionActor & lori.ServerLifecycleEventReceiver)
  """
  Mock server that immediately rejects the startup message with SQLSTATE
  53300 (too_many_connections), closing the connection without requesting
  authentication.
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  var _sent: Bool = false
  let _reader: _MockMessageReader = _MockMessageReader

  new create(auth: lori.TCPServerAuth, fd: U32) =>
    _tcp_connection = lori.TCPConnection.server(auth, fd, this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_received(data: Array[U8] iso) =>
    _reader.append(consume data)
    if not _sent then
      match _reader.read_startup_message()
      | let _: Array[U8] val =>
        _sent = true
        let err = _IncomingErrorResponseTestMessage(
          "FATAL", "53300",
          "sorry, too many clients already").bytes()
        _tcp_connection.send(err)
        _tcp_connection.close()
      end
    end

actor \nodoc\ _TooManyConnectionsTestNotify is SessionStatusNotify
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    _h.fail("Should not have authenticated.")
    _h.complete(false)

  be pg_session_connection_failed(s: Session,
    reason: ConnectionFailureReason)
  =>
    match reason
    | let r: TooManyConnections =>
      if r.response().code != "53300" then
        _h.fail("TooManyConnections carried wrong SQLSTATE code: "
          + r.response().code)
        _h.complete(false)
        return
      end
      if r.response().message.size() == 0 then
        _h.fail("TooManyConnections carried empty message.")
        _h.complete(false)
        return
      end
      _h.complete(true)
    else
      _h.fail("Expected TooManyConnections, got different reason.")
      _h.complete(false)
    end
