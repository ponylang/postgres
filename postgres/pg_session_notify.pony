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

  fun ref on_authenticated() =>
    """
    Called when we have successfully authenticated with the server.
    """
    None

  fun ref on_authentication_failed() =>
    """
    Called if we have failed to successfully authenicate with the server.
    """
    None
