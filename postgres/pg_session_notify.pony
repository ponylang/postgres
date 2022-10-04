interface tag SessionStatusNotify
  be pg_session_connected(session: Session) =>
    """
    Called when we have connected to the server but haven't yet tried to
    authenticate.
    """
    None

  be pg_session_connection_failed(session: Session) =>
    """
    Called when we have failed to connect to the server before attempting to
    authenticate.
    """
    None

  be pg_session_authenticated(session: Session) =>
    """
    Called when we have successfully authenticated with the server.
    """
    None

  be pg_session_authentication_failed(
    session: Session,
    reason: AuthenticationFailureReason)
  =>
    """
    Called if we have failed to successfully authenicate with the server.
    """
    None
