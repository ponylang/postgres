class val Field is Equatable[Field]
  """
  A single column value within a `Row`. Contains the column name and the
  value decoded via the `CodecRegistry` into a `FieldData` type. Built-in
  types include `Bool`, `I16`, `I32`, `I64`, `F32`, `F64`, `String`, `Bytea`,
  `PgArray`, `PgComposite`, `PgDate`, `PgTime`, `PgTimestamp`, `PgInterval`,
  and `None`
  (for NULL). Custom codecs may produce additional types implementing
  `FieldData`.
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
    | (None, None) => true
    | (let a: FieldData, let b: FieldData) => _FieldDataEq(a, b)
    end
