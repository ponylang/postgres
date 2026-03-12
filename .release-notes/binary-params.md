## Change PreparedQuery and NamedPreparedQuery parameters to typed FieldDataTypes

`PreparedQuery` and `NamedPreparedQuery` parameters changed from `Array[(String | None)] val` to `Array[FieldDataTypes] val`. Typed values (`I16`, `I32`, `I64`, `F32`, `F64`, `Bool`, `Array[U8] val`) are now sent in binary wire format with explicit type OIDs, while `String` and `None` continue to use text format with server-inferred types.

Binary encoding eliminates the text→binary conversion the server previously had to do for every typed parameter. It also removes a class of silent bugs where a string like `"42"` could be interpreted as different types depending on context.

Before:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as (String | None): "42"] end)
```

After:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE id = $1",
  recover val [as FieldDataTypes: I32(42)] end)
```

String parameters still work — use them for text values or when you want the server to infer the type:

```pony
let query = PreparedQuery("SELECT * FROM users WHERE name = $1",
  recover val [as FieldDataTypes: "Alice"] end)
```

If a parameter value can't be encoded (type mismatch with the codec), the query fails with `DataError` via `pg_query_failed` instead of being sent to the server.
