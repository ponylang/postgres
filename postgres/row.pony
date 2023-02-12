class val Row
  let fields: Array[Field] val

  new val create(fields': Array[Field] val) =>
    fields = fields'
