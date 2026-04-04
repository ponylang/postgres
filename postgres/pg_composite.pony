class val PgComposite is (FieldData & FieldDataEquatable & Equatable[PgComposite])
  """
  A PostgreSQL composite type (user-defined structured type created with
  `CREATE TYPE ... AS (...)`). Contains the composite's own type OID,
  per-field OIDs and names, and an ordered sequence of field values where
  each value is either a `FieldData` value or `None` (SQL NULL).

  Used for both decoding composites from query results and encoding
  composites as query parameters:

  ```pony
  // As a query parameter — use from_fields for safe construction
  let addr = PgComposite.from_fields(16400,
    recover val
      [as (String, U32, (FieldData | None)):
        ("street", 25, "123 Main St")
        ("city", 25, "Springfield")
        ("zip_code", 23, I32(62704))]
    end)
  session.execute(PreparedQuery("INSERT INTO users (home) VALUES ($1)",
    recover val [as FieldDataTypes: addr] end), receiver)

  // From a result field — positional access
  match field.value
  | let c: PgComposite =>
    match try c(0)? end
    | let street: String => // use street
    | None => // NULL field
    end
  end

  // From a result field — named access
  match field.value
  | let c: PgComposite =>
    match try c.field("city")? end
    | let city: String => // use city
    end
  end
  ```

  Register composite types with `CodecRegistry.with_composite_type()` before
  use. Unregistered composite OIDs fall back to `RawBytes` (binary format)
  or `String` (text format).
  """
  let type_oid: U32
  let field_oids: Array[U32] val
  let field_names: Array[String] val
  let fields: Array[(FieldData | None)] val

  new val create(type_oid': U32, field_oids': Array[U32] val,
    field_names': Array[String] val,
    fields': Array[(FieldData | None)] val) ?
  =>
    """
    Construct from pre-split parallel arrays. Used internally by the decode
    path where names, OIDs, and values arrive as separate arrays. Prefer
    `from_fields` for user construction.
    """
    if (field_oids'.size() != field_names'.size())
      or (field_oids'.size() != fields'.size())
    then
      error
    end
    type_oid = type_oid'
    field_oids = field_oids'
    field_names = field_names'
    fields = fields'

  new val from_fields(type_oid': U32,
    descriptors: Array[(String, U32, (FieldData | None))] val)
  =>
    """
    Construct from `(name, oid, value)` triples. This is the preferred
    constructor for user-created composites (query parameters), since it
    keeps each field's name, OID, and value together and eliminates the
    risk of misalignment across parallel arrays.

    ```pony
    let addr = PgComposite.from_fields(16400,
      recover val
        [as (String, U32, (FieldData | None)):
          ("street", 25, "123 Main St")
          ("city", 25, "Springfield")
          ("zip_code", 23, I32(62704))]
      end)
    ```
    """
    type_oid = type_oid'
    field_names = recover val
      let n = Array[String](descriptors.size())
      for (name, _, _) in descriptors.values() do
        n.push(name)
      end
      n
    end
    field_oids = recover val
      let o = Array[U32](descriptors.size())
      for (_, oid, _) in descriptors.values() do
        o.push(oid)
      end
      o
    end
    fields = recover val
      let f = Array[(FieldData | None)](descriptors.size())
      for (_, _, value) in descriptors.values() do
        f.push(value)
      end
      f
    end

  fun size(): USize =>
    """
    Number of fields in the composite.
    """
    fields.size()

  fun apply(i: USize): (FieldData | None) ? =>
    """
    Positional field access.
    """
    fields(i)?

  fun field(name: String): (FieldData | None) ? =>
    """
    Named field access. Searches `field_names` for a match and returns
    the corresponding value. Errors if the name is not found. If duplicate
    names exist (PostgreSQL allows this), returns the first match.
    """
    var i: USize = 0
    while i < field_names.size() do
      if field_names(i)? == name then
        return fields(i)?
      end
      i = i + 1
    end
    error

  fun eq(that: box->PgComposite): Bool =>
    if type_oid != that.type_oid then return false end
    if fields.size() != that.fields.size() then return false end
    try
      var i: USize = 0
      while i < fields.size() do
        if not _FieldDataEq.nullable(fields(i)?, that.fields(i)?) then
          return false
        end
        i = i + 1
      end
      true
    else
      _Unreachable()
      false
    end

  fun field_data_eq(that: FieldData box): Bool =>
    match that
    | let other: PgComposite box =>
      if type_oid != other.type_oid then return false end
      if fields.size() != other.fields.size() then return false end
      try
        var i: USize = 0
        while i < fields.size() do
          if not _FieldDataEq.nullable(fields(i)?, other.fields(i)?) then
            return false
          end
          i = i + 1
        end
        true
      else
        _Unreachable()
        false
      end
    else
      false
    end

  fun string(): String iso^ =>
    """
    PostgreSQL composite literal format: `(val1,val2,val3)`. Field values
    are double-quoted when they contain parentheses, commas, double-quotes,
    backslashes, or whitespace. Empty strings are quoted (`""`) to
    distinguish from NULL. NULL fields are unquoted empty positions.
    Double-quotes and backslashes inside values are escaped by doubling
    (`""` and `\\` respectively).
    """
    recover iso
      let s = String
      s.push('(')
      var first = true
      for elem in fields.values() do
        if first then first = false else s.push(',') end
        match elem
        | None => None // empty position = NULL
        | let fd: FieldData =>
          let v: String val = fd.string()
          if (v.size() == 0) or _needs_quoting(v) then
            s.push('"')
            for ch in v.values() do
              if ch == '"' then s.push('"') end
              if ch == '\\' then s.push('\\') end
              s.push(ch)
            end
            s.push('"')
          else
            s.append(v)
          end
        end
      end
      s.push(')')
      s
    end

  fun tag _needs_quoting(v: String val): Bool =>
    for ch in v.values() do
      if (ch == ',') or (ch == '(') or (ch == ')')
        or (ch == '"') or (ch == '\\')
        or (ch == ' ') or (ch == '\t') or (ch == '\n') or (ch == '\r')
      then
        return true
      end
    end
    false
