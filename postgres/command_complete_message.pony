class val CommandCompleteMessage
  """
  A command has finished running. The message contains information about final
  details.
  """
  let id: String
  let value: USize

  new val create(id': String, value': USize) =>
    id = id'
    value = value'
