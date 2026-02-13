class val Rows is Equatable[Rows]
  let _rows: Array[Row] val

  new val create(rows': Array[Row] val) =>
    _rows = rows'

  fun size(): USize =>
    """
    Returns the number of rows.
    """
    _rows.size()

  fun apply(i: USize): Row ? =>
    """
    Returns the `i`th row if it exists. Otherwise, throws an error.
    """
    _rows(i)?

  fun eq(that: box->Rows): Bool =>
    """
    Two Rows are equal when they have the same number of rows and each
    corresponding pair of rows is equal. Row order matters: the same rows
    in a different order are not equal.
    """
    if size() != that.size() then return false end
    try
      var i: USize = 0
      while i < size() do
        if apply(i)? != that(i)? then return false end
        i = i + 1
      end
      true
    else
      false
    end

  fun values(): RowIterator =>
    """
    Returns an iterator over the rows.
    """
    RowIterator._create(_rows)

class RowIterator is Iterator[Row]
  let _array: Array[Row] val
  var _i: USize

  new _create(array: Array[Row] val) =>
    _array = array
    _i = 0

  fun has_next(): Bool =>
    _i < _array.size()

  fun ref next(): Row ? =>
    _array(_i = _i + 1)?

  fun ref rewind(): RowIterator =>
    _i = 0
    this

primitive _RowsBuilder
  fun apply(rows': Array[Array[(String|None)] val] val,
    row_descriptions': Array[(String, U32)] val): Rows ?
  =>
    let rows = recover iso Array[Row] end
    for row in rows'.values() do
      let fields = recover iso Array[Field] end
      for (i, v) in row.pairs() do
        let desc = row_descriptions'(i)?
        let field_name = desc._1
        let field_type = desc._2
        let field_value = _field_to_type(v, field_type)?
        let field = Field(field_name, field_value)
        fields.push(field)
      end
      rows.push(Row(consume fields))
    end
    Rows(consume rows)

  fun _field_to_type(field: (String | None), type_id: U32): FieldDataTypes ? =>
    match field
    | let f: String =>
      match type_id
      | 16 => f.at("t")
      | 17 => _decode_hex_bytea(f)?
      | 20 => f.i64()?
      | 21 => f.i16()?
      | 23 => f.i32()?
      | 700 => f.f32()?
      | 701 => f.f64()?
      else
        f
      end
    | None =>
      None
    end

  fun _decode_hex_bytea(s: String): Array[U8] val ? =>
    if (s.size() < 2) or (s(0)? != '\\') or (s(1)? != 'x') then error end
    let hex_len = s.size() - 2
    if (hex_len % 2) != 0 then error end
    recover val
      let result = Array[U8](hex_len / 2)
      var i: USize = 2
      while i < s.size() do
        let hi = _hex_digit(s(i)?)?
        let lo = _hex_digit(s(i + 1)?)?
        result.push((hi * 16) + lo)
        i = i + 2
      end
      result
    end

  fun _hex_digit(c: U8): U8 ? =>
    if (c >= '0') and (c <= '9') then c - '0'
    elseif (c >= 'a') and (c <= 'f') then (c - 'a') + 10
    elseif (c >= 'A') and (c <= 'F') then (c - 'A') + 10
    else error
    end
