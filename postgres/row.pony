class val Row is Equatable[Row]
  """
  A single row from a query result. Contains an ordered array of `Field`
  values corresponding to the columns in the `RowDescription`.
  """
  let fields: Array[Field] val

  new val create(fields': Array[Field] val) =>
    fields = fields'

  fun eq(that: box->Row): Bool =>
    """
    Two rows are equal when they have the same number of fields and each
    corresponding pair of fields is equal. Field order matters: the same
    fields in a different order are not equal.
    """
    if fields.size() != that.fields.size() then return false end
    try
      var i: USize = 0
      while i < fields.size() do
        if fields(i)? != that.fields(i)? then return false end
        i = i + 1
      end
      true
    else
      false
    end
