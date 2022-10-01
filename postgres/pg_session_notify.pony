interface PgSessionNotify
  fun ref on_connected() =>
    """
    Called when we have connected to the server but haven't yet tried to
    authenticate.
    """
    None

  fun ref on_connection_failed() =>
    """
    Called when we have failed to connect to the server before attempting to
    authenticate.
    """
    None
