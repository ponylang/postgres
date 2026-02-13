interface tag CopyInReceiver
  """
  Receives the result of a `Session.copy_in()` call. The session is passed to
  each callback so receivers can execute follow-up operations without needing
  to store a session reference.
  """
  be pg_copy_ready(session: Session)
    """
    Called when the session is ready to accept the next chunk of COPY data.
    Called once when the server enters COPY mode, and again after each
    `send_copy_data()` call.

    The receiver should respond by calling exactly one of:
    - `session.send_copy_data(data)` to send a chunk (triggers another
      `pg_copy_ready`)
    - `session.finish_copy()` to signal successful end of data
    - `session.abort_copy(reason)` to abort the operation

    The flow control guarantee depends on the receiver calling exactly one of
    these methods per `pg_copy_ready`. Calling `send_copy_data` without a
    preceding `pg_copy_ready` bypasses the bounded-memory guarantee.
    """

  be pg_copy_complete(session: Session, count: USize)
    """
    Called when the COPY operation completes successfully. The count is the
    number of rows copied.
    """

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when the COPY operation fails. The failure is either a server error
    (ErrorResponseMessage) or a client-side error (ClientQueryError) such as
    the session being closed or not yet authenticated.
    """
