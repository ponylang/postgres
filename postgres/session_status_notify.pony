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

  be pg_notification(session: Session, notification: Notification) =>
    """
    Called when the server delivers a LISTEN/NOTIFY notification. Subscribe
    to notifications by executing LISTEN via Session.execute(). Notifications
    arrive asynchronously between query cycles.
    """
    None

  be pg_notice(session: Session, notice: NoticeResponseMessage) =>
    """
    Called when the server sends a non-fatal informational message
    (NoticeResponse). Common triggers include `DROP TABLE IF EXISTS` on a
    nonexistent table and `RAISE NOTICE` from PL/pgSQL. Notices can arrive
    in any connected state, including during authentication.
    """
    None

  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    """
    Called when the server reports a runtime parameter value. PostgreSQL sends
    these during connection startup for all reporting parameters (server_version,
    client_encoding, standard_conforming_strings, etc.) and again whenever a
    SET command changes one.

    The callback has a default no-op implementation, so existing code is
    unaffected.
    """
    None

  be pg_session_shutdown(session: Session) =>
    """
    Called when a session ends.
    """
