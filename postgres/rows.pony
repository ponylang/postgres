class val Rows
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

  fun values(): RowIterator =>
    """
    Returns an iterator over the rows.
    ""
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

// TODO need tests for all this
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
        let field_value = field_to_type(v, field_type)?
        let field = Field(field_name, field_value)
        fields.push(field)
      end
      rows.push(Row(consume fields))
    end
    Rows(consume rows)

  fun field_to_type(field: (String | None), type_id: U32): FieldDataTypes ? =>
    match field
    | let f: String =>
      match type_id
      | 16 => f.at("t")
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
