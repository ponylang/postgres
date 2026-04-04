primitive _FieldDataEq
  """
  Shared equality logic for `FieldData` values. Used by `Field.eq()`,
  `PgArray.eq()`, and `PgComposite.eq()` to avoid duplicating the
  type-matching arms.
  """
  fun apply(a: FieldData, b: FieldData): Bool =>
    match (a, b)
    | (let x: Bytea, let y: Bytea) => x == y
    | (let x: RawBytes, let y: RawBytes) => x == y
    | (let x: Bool, let y: Bool) => x == y
    | (let x: F32, let y: F32) => x == y
    | (let x: F64, let y: F64) => x == y
    | (let x: I16, let y: I16) => x == y
    | (let x: I32, let y: I32) => x == y
    | (let x: I64, let y: I64) => x == y
    | (let x: PgArray, let y: PgArray) => x == y
    | (let x: PgComposite, let y: PgComposite) => x == y
    | (let x: PgDate, let y: PgDate) => x == y
    | (let x: PgInterval, let y: PgInterval) => x == y
    | (let x: PgTime, let y: PgTime) => x == y
    | (let x: PgTimestamp, let y: PgTimestamp) => x == y
    | (let x: String, let y: String) => x == y
    | (let x: FieldDataEquatable, let y: FieldData) =>
      x.field_data_eq(y)
    else
      false
    end

  fun nullable(a: (FieldData | None), b: (FieldData | None)): Bool =>
    """
    Equality for nullable field data values — used by `PgArray.eq()` and
    `PgComposite.eq()`.
    Two `None` values are equal. When both are `FieldData`, delegates to
    `apply()` which returns `false` for unmatched types (including `None`
    passed as `FieldData`).
    """
    match (a, b)
    | (None, None) => true
    | (let x: FieldData, let y: FieldData) => apply(x, y)
    end
