class val Rows is Equatable[Rows]
  """
  An ordered collection of `Row` values from a query result. Supports indexed
  access via `apply()` and iteration via `values()`.
  """
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
  """
  An iterator over the rows in a `Rows` collection. Supports rewinding to
  iterate multiple times. Obtained via `Rows.values()`.
  """
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
  fun apply(rows': Array[Array[(Array[U8] val | None)] val] val,
    row_descriptions': Array[(String, U32, U16)] val,
    registry: CodecRegistry): Rows ?
  =>
    let rows = recover iso Array[Row] end
    for row in rows'.values() do
      let fields = recover iso Array[Field] end
      for (i, v) in row.pairs() do
        let desc = row_descriptions'(i)?
        let field_name = desc._1
        let oid = desc._2
        let format_code = desc._3
        let field_value: FieldDataTypes = match v
        | let data: Array[U8] val => registry.decode(oid, format_code, data)
        | None => None
        end
        fields.push(Field(field_name, field_value))
      end
      rows.push(Row(consume fields))
    end
    Rows(consume rows)
