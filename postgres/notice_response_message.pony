class val NoticeResponseMessage
  """
  A non-fatal informational message from the server. PostgreSQL sends these
  for conditions like `DROP TABLE IF EXISTS` on a nonexistent table,
  `RAISE NOTICE` from PL/pgSQL, or implicit index creation. Notices use
  the same field structure as `ErrorResponseMessage` but indicate conditions
  that do not abort the current operation.

  Delivered via `SessionStatusNotify.pg_notice()`.
  """
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
