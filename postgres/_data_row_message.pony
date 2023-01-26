class val _DataRowMessage
  let columns: Array[(String|None)] val

  new val create(columns': Array[(String|None)] val) =>
    columns = columns'
