## Add connection timeout

You can now set a timeout on the TCP connection phase by passing a `connection_timeout` to `ServerConnectInfo`. If the server is unreachable within the given duration, `pg_session_connection_failed` fires with `ConnectionFailedTimeout` instead of hanging indefinitely. Construct the timeout with `lori.MakeConnectionTimeout(milliseconds)`.

```pony
match lori.MakeConnectionTimeout(5000)
| let ct: lori.ConnectionTimeout =>
  let session = Session(
    ServerConnectInfo(auth, host, port
      where connection_timeout' = ct),
    DatabaseConnectInfo(username, password, database),
    notify)
end
```

Without a connection timeout (the default), connection attempts have no time bound and rely on the operating system's TCP timeout behavior.
