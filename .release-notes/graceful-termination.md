## Send Terminate message before closing TCP connection

`Session.close()` now sends a Terminate message to the PostgreSQL server before closing the TCP connection. Previously, the connection was hard-closed without notifying the server, which could leave server-side resources (session state, prepared statements, temp tables) lingering until the server detected the broken connection on its next I/O attempt.

No code changes are needed — `Session.close()` handles this automatically.
