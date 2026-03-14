class val PgArray is (FieldData & FieldDataEquatable & Equatable[PgArray])
  """
  A 1-dimensional PostgreSQL array. Contains the element type OID and an
  ordered sequence of elements, where each element is either a `FieldData`
  value or `None` (SQL NULL).

  Used for both decoding arrays from query results and encoding arrays as
  query parameters:

  ```pony
  // As a query parameter
  let arr = PgArray(23,
    recover val [as (FieldData | None): I32(1); I32(2); None; I32(4)] end)
  session.execute(PreparedQuery("SELECT $1::int4[]",
    recover val [as FieldDataTypes: arr] end), receiver)

  // From a result field
  match field.value
  | let a: PgArray =>
    for elem in a.elements.values() do
      match elem
      | let v: I32 => // use v
      | None => // NULL element
      end
    end
  end
  ```
  """
  let element_oid: U32
  let elements: Array[(FieldData | None)] val

  new val create(element_oid': U32,
    elements': Array[(FieldData | None)] val)
  =>
    element_oid = element_oid'
    elements = elements'

  fun size(): USize =>
    """
    Number of elements in the array.
    """
    elements.size()

  fun apply(i: USize): (FieldData | None) ? =>
    """
    Indexed element access.
    """
    elements(i)?

  fun eq(that: box->PgArray): Bool =>
    if element_oid != that.element_oid then return false end
    if elements.size() != that.elements.size() then return false end
    try
      var i: USize = 0
      while i < elements.size() do
        if not _FieldDataEq.nullable(elements(i)?, that.elements(i)?) then
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
    | let other: PgArray box =>
      if element_oid != other.element_oid then return false end
      if elements.size() != other.elements.size() then return false end
      try
        var i: USize = 0
        while i < elements.size() do
          if not _FieldDataEq.nullable(elements(i)?, other.elements(i)?)
          then
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
    PostgreSQL array literal format: `{1,2,NULL,4}`. Elements containing
    commas, braces, quotes, backslashes, or whitespace are double-quoted
    with internal backslash escaping. Empty string elements are quoted
    (`""`) to distinguish from NULL. `None` elements render as unquoted
    `NULL`.
    """
    recover iso
      let s = String
      s.push('{')
      var first = true
      for elem in elements.values() do
        if first then first = false else s.push(',') end
        match elem
        | None => s.append("NULL")
        | let fd: FieldData =>
          let v: String val = fd.string()
          if (v.size() == 0) or _needs_quoting(v) then
            s.push('"')
            for ch in v.values() do
              if (ch == '"') or (ch == '\\') then s.push('\\') end
              s.push(ch)
            end
            s.push('"')
          else
            s.append(v)
          end
        end
      end
      s.push('}')
      s
    end

  fun tag _needs_quoting(v: String val): Bool =>
    for ch in v.values() do
      if (ch == ',') or (ch == '{') or (ch == '}')
        or (ch == '"') or (ch == '\\')
        or (ch == ' ') or (ch == '\t') or (ch == '\n') or (ch == '\r')
      then
        return true
      end
    end
    // Case-insensitive check for "NULL"
    if v.size() == 4 then
      try
        let c0 = v(0)?
        let c1 = v(1)?
        let c2 = v(2)?
        let c3 = v(3)?
        if ((c0 == 'N') or (c0 == 'n'))
          and ((c1 == 'U') or (c1 == 'u'))
          and ((c2 == 'L') or (c2 == 'l'))
          and ((c3 == 'L') or (c3 == 'l'))
        then
          return true
        end
      end
    end
    false
