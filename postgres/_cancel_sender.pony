use lori = "lori"
use "ssl/net"

actor _CancelSender is (lori.TCPConnectionActor & lori.ClientLifecycleEventReceiver)
  """
  Fire-and-forget actor that sends a CancelRequest on a separate TCP
  connection. PostgreSQL requires cancel requests on a different connection
  from the one executing the query. No response is expected on the cancel
  connection — the result (if any) arrives as an ErrorResponse on the
  original session connection.

  When `SSLRequired` is active, performs SSL negotiation before sending the
  CancelRequest — mirroring what the main Session connection does. If the
  server refuses SSL or the TLS handshake fails, the cancel is silently
  abandoned (fire-and-forget semantics).
  """
  var _tcp_connection: lori.TCPConnection = lori.TCPConnection.none()
  let _process_id: I32
  let _secret_key: I32
  let _info: ServerConnectInfo

  new create(info: ServerConnectInfo, process_id: I32, secret_key: I32) =>
    _process_id = process_id
    _secret_key = secret_key
    _info = info
    _tcp_connection = lori.TCPConnection.client(
      info.auth, info.host, info.service, "", this, this)

  fun ref _connection(): lori.TCPConnection =>
    _tcp_connection

  fun ref _on_connected() =>
    match _info.ssl_mode
    | SSLDisabled =>
      _send_cancel_and_close()
    | let _: SSLRequired =>
      // CVE-2021-23222 mitigation: expect exactly 1 byte for SSL response.
      try _tcp_connection.expect(1)? end
      _tcp_connection.send(_FrontendMessage.ssl_request())
    end

  fun ref _on_received(data: Array[U8] iso) =>
    // Only called during SSL negotiation — server responds 'S' or 'N'.
    try
      if data(0)? == 'S' then
        match _info.ssl_mode
        | let req: SSLRequired =>
          match _tcp_connection.start_tls(req.ctx, _info.host)
          | None => None  // Handshake started, wait for _on_tls_ready
          | let _: lori.StartTLSError =>
            _tcp_connection.close()
          end
        end
      else
        // 'N' or junk — server refused SSL, silently give up
        _tcp_connection.close()
      end
    else
      _tcp_connection.close()
    end

  fun ref _on_tls_ready() =>
    // Reset expect from 1 back to 0 (same pattern as _SessionSSLNegotiating)
    try _tcp_connection.expect(0)? end
    _send_cancel_and_close()

  fun ref _on_tls_failure() =>
    // Fire-and-forget: TLS handshake failed, silently give up.
    // Lori follows this with _on_closed() (default no-op), so no
    // additional cleanup needed.
    None

  fun ref _send_cancel_and_close() =>
    _tcp_connection.send(
      _FrontendMessage.cancel_request(_process_id, _secret_key))
    _tcp_connection.close()
