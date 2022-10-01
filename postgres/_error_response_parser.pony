primitive _ErrorResponseParser
  fun apply(payload: Array[U8] val): _ErrorMessage ? =>
    var code = ""
    var code_index: USize = 0

    while (payload(code_index)? > 0) do
      let field_type = payload(code_index)?

      // Find the field terminator. All fields are null terminated.
      let null_index = payload.find(0, code_index)?
      let field_index = code_index + 1
      let field_data = String.from_array(recover
          payload.slice(field_index, null_index)
        end)

      if field_type == _ErrorResponseField.code() then
        code = field_data
      end

      code_index = null_index + 1
    end

    _ErrorMessage(code)
