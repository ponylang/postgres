class val Field
  let name: String
  let value: FieldDataTypes

  new val create(name': String, value': FieldDataTypes) =>
    name = name'
    value = value'
