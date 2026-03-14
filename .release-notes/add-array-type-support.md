## Add 1-dimensional array type support

PostgreSQL array columns are now automatically decoded into `PgArray` values, and `PgArray` can be used as a query parameter. All built-in element types are supported: bool, bytea, int2, int4, int8, float4, float8, text, date, time, timestamp, timestamptz, interval, uuid, jsonb, numeric, and text-like types (char, name, xml, bpchar, varchar).

```pony
// Decoding from query results
match field.value
| let a: PgArray =>
  for elem in a.elements.values() do
    match elem
    | let v: I32 => // use v
    | None => // NULL element
    end
  end
end

// Encoding as a query parameter
let arr = PgArray(23,
  recover val [as (FieldData | None): I32(1); I32(2); None; I32(4)] end)
session.execute(PreparedQuery("SELECT $1::int4[]",
  recover val [as FieldDataTypes: arr] end), receiver)
```

`PgArray` works with both `SimpleQuery` (text format) and `PreparedQuery`/`NamedPreparedQuery` (binary format). Custom array types are supported via `CodecRegistry.with_array_type()`:

```pony
let registry = CodecRegistry
  .with_codec(600, PointBinaryCodec)
  .with_array_type(1017, 600)
```

Multi-dimensional arrays are not supported and will fall back to `String` (text format) or `RawBytes` (binary format).
