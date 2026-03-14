## Change extended query results to binary format with typed temporal values

Extended query results (`PreparedQuery`, `NamedPreparedQuery`, streaming, pipelining) now use PostgreSQL's binary wire format instead of text. This means result values are decoded from their native binary representation rather than parsed from text strings. SimpleQuery results are unaffected — they continue using text format.

This change expands `FieldDataTypes` with four new temporal types and changes the decode type for some PostgreSQL OIDs:

- `date` columns now decode to `PgDate` (was `String`)
- `time` columns now decode to `PgTime` (was `String`)
- `timestamp` and `timestamptz` columns now decode to `PgTimestamp` (was `String`)
- `interval` columns now decode to `PgInterval` (was `String`)
- `oid`, `numeric`, `uuid`, and `jsonb` columns now decode to `String` with proper formatting (was `String` in text mode; now binary-decoded)
- Columns with unknown OIDs now decode to `Array[U8] val` (was `String` in text mode)

Any code with exhaustive `match` on `FieldDataTypes` or `field.value` must add arms for the new temporal types.

Before:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let s: String => _env.out.print(field.name + ": " + s)
        | let i: I32 => _env.out.print(field.name + ": " + i.string())
        | let b: Bool => _env.out.print(field.name + ": " + b.string())
        | let v: Array[U8] val =>
          _env.out.print(field.name + ": bytes")
        | None => _env.out.print(field.name + ": NULL")
        end
      end
    end
  end
```

After:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let rs: ResultSet =>
    for row in rs.rows().values() do
      for field in row.fields.values() do
        match field.value
        | let s: String => _env.out.print(field.name + ": " + s)
        | let i: I32 => _env.out.print(field.name + ": " + i.string())
        | let b: Bool => _env.out.print(field.name + ": " + b.string())
        | let v: Array[U8] val =>
          _env.out.print(field.name + ": bytes")
        | let t: PgTimestamp => _env.out.print(field.name + ": " + t.string())
        | let t: PgDate => _env.out.print(field.name + ": " + t.string())
        | let t: PgTime => _env.out.print(field.name + ": " + t.string())
        | let t: PgInterval => _env.out.print(field.name + ": " + t.string())
        | None => _env.out.print(field.name + ": NULL")
        end
      end
    end
  end
```

The temporal types store their raw PostgreSQL values and provide `string()` methods that format them in standard PostgreSQL output format. `PgTimestamp` and `PgDate` support infinity via `I64.max_value()`/`I64.min_value()` and `I32.max_value()`/`I32.min_value()` respectively. `PgTime` uses constrained types — construct via `MakePgTimeMicroseconds` to validate the microseconds value, then pass the result to `PgTime.create()`.
