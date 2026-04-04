## Add composite type support

The driver now supports PostgreSQL composite types (user-defined structured types created with `CREATE TYPE ... AS (...)`). Composite values are decoded as `PgComposite` in query results and can be sent as query parameters.

Register composite types with `CodecRegistry.with_composite_type()`:

```pony
// CREATE TYPE address AS (street text, city text, zip_code int4)
// OID discovered via: SELECT oid FROM pg_type WHERE typname = 'address'

let registry = CodecRegistry
  .with_composite_type(16400,
    recover val
      [as (String, U32): ("street", 25); ("city", 25); ("zip_code", 23)]
    end)?
  .with_array_type(16401, 16400)?  // address[]
```

Access fields by position or name:

```pony
match field.value
| let addr: PgComposite =>
  match try addr(0)? end       // positional
  | let street: String => // ...
  end
  match try addr.field("city")? end  // named
  | let city: String => // ...
  end
end
```

Send composites as query parameters using `from_fields` for safe construction:

```pony
let addr = PgComposite.from_fields(16400,
  recover val
    [as (String, U32, (FieldData | None)):
      ("street", 25, "123 Main St")
      ("city", 25, "Springfield")
      ("zip_code", 23, I32(62704))]
  end)
session.execute(PreparedQuery("INSERT INTO users (home) VALUES ($1)",
  recover val [as FieldDataTypes: addr] end), receiver)
```

Nested composites and composite arrays are supported. Both `PreparedQuery` (binary format) and `SimpleQuery` (text format) decode composites.
