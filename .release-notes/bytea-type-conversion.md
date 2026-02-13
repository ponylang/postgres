## Add bytea type conversion

PostgreSQL `bytea` columns are now automatically decoded from hex format into `Array[U8] val`. Previously, bytea values were returned as raw hex strings (e.g., `\x48656c6c6f`). They are now decoded into byte arrays that you can work with directly.

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let bytes: Array[U8] val =>
          // Decoded bytes — e.g., [72; 101; 108; 108; 111] for "Hello"
          for b in bytes.values() do
            _env.out.print("byte: " + b.string())
          end
        end
      end
    end
  end
```

Existing code is unaffected — if your `match` on `field.value` doesn't include an `Array[U8] val` arm, bytea values simply won't match any branch (Pony's match is non-exhaustive).
