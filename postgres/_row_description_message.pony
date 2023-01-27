class val _RowDescriptionMessage
  let columns: Array[(String, U32)] val

  new val create(columns': Array[(String, U32)] val) =>
    columns = columns'
