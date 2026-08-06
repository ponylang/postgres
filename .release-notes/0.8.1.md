## Fix a number of SSL bugs

Fixed multiple bugs affecting SSL support, including handshake failures being reported as authentication failures, data being silently dropped on large writes and when encryption fails, and connections being closed by unrelated SSL failures elsewhere.

## Fix additional SSL connection bugs

Fixed additional bugs in SSL connection handling that could cause handshake failures to be misreported, data to be silently dropped during encrypted writes, and one connection's SSL failure to close a different connection.

## Fix a macOS bug where setting up a connection could close an unrelated file descriptor

On macOS, setting up a connection could close one of its own file descriptors twice. The operating system can hand that descriptor number to something else in between, so the second close lands on whatever got it — an unrelated connection or file closes silently. Connecting to a host that resolves to more than one address (including `localhost`) is the likeliest way to hit it. The same cleanup also miscounted outstanding connection attempts, which could abandon a working attempt and report the connection as failed, or leave a connection that was asked to close gracefully never finishing. Linux and Windows were not affected.

