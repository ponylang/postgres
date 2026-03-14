interface val FieldData
  """
  Open result type for decoded column values. All built-in types (`Bool`,
  `I16`, `I32`, `I64`, `F32`, `F64`, `String`, `PgDate`, `PgTime`,
  `PgTimestamp`, `PgInterval`, `Bytea`) conform structurally via `Stringable`.
  Custom codecs return their own types implementing this interface.

  The encode path (`FieldDataTypes`) remains a closed union because parameters
  must map to known PostgreSQL OIDs for the wire protocol. The decode path is
  open because result values just need to be readable.
  """
  fun string(): String iso^

interface val FieldDataEquatable
  """
  Opt-in equality for custom `FieldData` types used in `Field.eq()`. Built-in
  types use explicit match arms. Custom types implement this interface to
  participate in field equality comparisons.
  """
  fun field_data_eq(that: FieldData box): Bool
