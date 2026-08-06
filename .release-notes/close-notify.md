## Send TLS close_notify on graceful close

Closing a TLS connection now sends a `close_notify` alert before the TCP shutdown. Without it, the peer could not distinguish a clean close from a truncated stream (RFC 8446 §6.1).
