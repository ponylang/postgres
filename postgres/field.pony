class val Field is Equatable[Field]
  """
  A single column value within a `Row`. Contains the column name and the
  value decoded via the `CodecRegistry` into a `FieldData` type. Built-in
  types include `Bool`, `I16`, `I32`, `I64`, `F32`, `F64`, `String`, `Bytea`,
  `PgDate`, `PgTime`, `PgTimestamp`, `PgInterval`, and `None` (for NULL).
  Custom codecs may produce additional types implementing `FieldData`.
  """
  let name: String
  let value: FieldData

  new val create(name': String, value': FieldData) =>
    name = name'
    value = value'

  fun eq(that: box->Field): Bool =>
    """
    Two fields are equal when they have the same name and the same value.
    Values must be the same type and compare equal using the type's own
    equality. Custom types that implement `FieldDataEquatable` participate
    in equality; custom types without it are never equal.
    """
    if name != that.name then return false end
    match (value, that.value)
    | (let a: Bytea, let b: Bytea) => a == b
    | (let a: RawBytes, let b: RawBytes) => a == b
    | (let a: Bool, let b: Bool) => a == b
    | (let a: F32, let b: F32) => a == b
    | (let a: F64, let b: F64) => a == b
    | (let a: I16, let b: I16) => a == b
    | (let a: I32, let b: I32) => a == b
    | (let a: I64, let b: I64) => a == b
    | (None, None) => true
    | (let a: PgDate, let b: PgDate) => a == b
    | (let a: PgInterval, let b: PgInterval) => a == b
    | (let a: PgTime, let b: PgTime) => a == b
    | (let a: PgTimestamp, let b: PgTimestamp) => a == b
    | (let a: String, let b: String) => a == b
    | (let a: FieldDataEquatable, let b: FieldData) =>
      a.field_data_eq(b)
    else
      false
    end
