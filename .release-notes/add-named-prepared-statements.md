## Add named prepared statement support

You can now create server-side named prepared statements with `Session.prepare()`, execute them with `NamedPreparedQuery`, and destroy them with `Session.close_statement()`. Named statements are parsed once and can be executed multiple times with different parameters, avoiding repeated parsing overhead.

```pony
// Prepare a named statement
session.prepare("find_user", "SELECT * FROM users WHERE id = $1", receiver)

// In the PrepareReceiver callback:
be pg_statement_prepared(name: String) =>
  // Execute with different parameters
  session.execute(
    NamedPreparedQuery("find_user",
      recover val [as (String | None): "42"] end),
    result_receiver)

// Clean up when done
session.close_statement("find_user")
```

The `Query` union type now includes `NamedPreparedQuery`, so exhaustive matches on `Query` need a new branch:

```pony
match query
| let sq: SimpleQuery => sq.string
| let pq: PreparedQuery => pq.string
| let nq: NamedPreparedQuery => nq.name
end
```
