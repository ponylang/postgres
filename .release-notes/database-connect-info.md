## Change Session constructor to accept DatabaseConnectInfo

`Session.create()` now takes a `DatabaseConnectInfo` instead of individual `user`, `password`, and `database` string parameters. `DatabaseConnectInfo` groups these authentication parameters into a single immutable value, matching the pattern established by `ServerConnectInfo`.

Before:

```pony
let session = Session(
  ServerConnectInfo(auth, host, port),
  notify,
  username,
  password,
  database)
```

After:

```pony
let session = Session(
  ServerConnectInfo(auth, host, port),
  notify,
  DatabaseConnectInfo(username, password, database))
```
