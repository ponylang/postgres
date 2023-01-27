class val Rows
  let rows: Array[Row] val

  new val create(rows': Array[Row] val) =>
    rows = rows'

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
