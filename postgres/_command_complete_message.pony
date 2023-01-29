class val _CommandCompleteMessage
  """
  A command has finished running. The message contains information about final
  details.
  """
  let _id: String

  new val create(id': String) =>
    _id = id'
