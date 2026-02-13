class val ErrorResponseMessage
  let severity: String
  let localized_severity: (String | None)
  let code: String
  let message: String
  let detail: (String | None)
  let hint: (String | None)
  let position: (String | None)
  let internal_position: (String | None)
  let internal_query: (String | None)
  let error_where: (String | None)
  let schema_name: (String | None)
  let table_name: (String | None)
  let column_name: (String | None)
  let data_type_name: (String | None)
  let constraint_name: (String | None)
  let file: (String | None)
  let line: (String | None)
  let routine: (String | None)

  new val create(severity': String,
    localized_severity': (String | None),
    code': String,
    message': String,
    detail': (String | None),
    hint': (String | None),
    position': (String | None),
    internal_position': (String | None),
    internal_query': (String | None),
    error_where': (String | None),
    schema_name': (String | None),
    table_name': (String | None),
    column_name': (String | None),
    data_type_name': (String | None),
    constraint_name': (String | None),
    file': (String | None),
    line': (String | None),
    routine': (String | None))
  =>
    severity = severity'
    localized_severity = localized_severity'
    code = code'
    message = message'
    detail = detail'
    hint = hint'
    position = position'
    internal_position = internal_position'
    internal_query = internal_query'
    error_where = error_where'
    schema_name = schema_name'
    table_name = table_name'
    column_name = column_name'
    data_type_name = data_type_name'
    constraint_name = constraint_name'
    file = file'
    line = line'
    routine = routine'

class _ResponseFieldBuilder
  var severity: (String | None) = None
  var localized_severity: (String | None) = None
  var code: (String | None) = None
  var message: (String | None) = None
  var detail: (String | None) = None
  var hint: (String | None) = None
  var position: (String | None) = None
  var internal_position: (String | None) = None
  var internal_query: (String | None) = None
  var error_where: (String | None) = None
  var schema_name: (String | None) = None
  var table_name: (String | None) = None
  var column_name: (String | None) = None
  var data_type_name: (String | None) = None
  var constraint_name: (String | None) = None
  var file: (String | None) = None
  var line: (String | None) = None
  var routine: (String | None) = None

  new create() =>
    None

  fun ref build_error(): ErrorResponseMessage ? =>
    // Three fields are required to build. All others are optional.
    let s = severity as String
    let c = code as String
    let m = message as String

    ErrorResponseMessage(s,
      localized_severity,
      c,
      m,
      detail,
      hint,
      position,
      internal_position,
      internal_query,
      error_where,
      schema_name,
      table_name,
      column_name,
      data_type_name,
      constraint_name,
      file,
      line,
      routine)

  fun ref build_notice(): NoticeResponseMessage ? =>
    let s = severity as String
    let c = code as String
    let m = message as String

    NoticeResponseMessage(s,
      localized_severity,
      c,
      m,
      detail,
      hint,
      position,
      internal_position,
      internal_query,
      error_where,
      schema_name,
      table_name,
      column_name,
      data_type_name,
      constraint_name,
      file,
      line,
      routine)





