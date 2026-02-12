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

  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    """
    Called when the server reports its transaction status via ReadyForQuery.
    Fires after every query cycle completes, including the initial ready
    signal after authentication. The status indicates whether the session is
    idle, in a transaction block, or in a failed transaction.
    """
    None

  be pg_session_shutdown(session: Session) =>
    """
    Called when a session ends.
    """
