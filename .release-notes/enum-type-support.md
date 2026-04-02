## Add enum type support

PostgreSQL enum columns return `RawBytes` when queried with `PreparedQuery` (binary format) because the driver doesn't recognize their dynamically-assigned OIDs. `SimpleQuery` (text format) already returns `String` via the unknown-OID fallback, but binary format had no equivalent.

`CodecRegistry.with_enum_type(oid)` registers an enum OID so both text and binary formats decode as `String`:

```pony
// Discover the OID once (e.g., SELECT oid FROM pg_type WHERE typname = 'mood')
let registry = CodecRegistry.with_enum_type(12345)?
let session = Session(server_info, db_info, notify where registry = registry)
```

Multiple enums and enum arrays compose naturally:

```pony
let registry = CodecRegistry
  .with_enum_type(12345)?          // mood
  .with_enum_type(12346)?          // color
  .with_array_type(12350, 12345)?  // mood[]
```

Enum values arrive as `String` in query results. No changes to `FieldDataTypes`, `Session`, or the `Codec` interface.
