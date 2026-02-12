class val Notification
  """
  A notification received via PostgreSQL's LISTEN/NOTIFY mechanism. Contains
  the channel name, payload string, and process ID of the notifying backend.

  Subscribe to notifications by executing `LISTEN channel_name` via
  `Session.execute()`. Unsubscribe with `UNLISTEN channel_name` or
  `UNLISTEN *` to remove all subscriptions.
  """
  let channel: String
  let payload: String
  let pid: I32

  new val create(channel': String, payload': String, pid': I32) =>
    channel = channel'
    payload = payload'
    pid = pid'
