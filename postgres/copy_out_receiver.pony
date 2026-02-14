interface tag CopyOutReceiver
  """
  Receives the result of a `Session.copy_out()` call. The session is passed to
  each callback so receivers can execute follow-up operations without needing
  to store a session reference.
  """
  be pg_copy_data(session: Session, data: Array[U8] val)
    """
    Called for each chunk of data received from the server during a
    COPY TO STDOUT operation. Data chunks do not necessarily align with row
    boundaries — the server may split or combine rows across chunks. The
    receiver should append each chunk to an output buffer or process it
    incrementally.
    """

  be pg_copy_complete(session: Session, count: USize)
    """
    Called when the COPY operation completes successfully. The count is the
    number of rows exported.
    """

  be pg_copy_failed(session: Session,
    failure: (ErrorResponseMessage | ClientQueryError))
    """
    Called when the COPY operation fails. The failure is either a server error
    (ErrorResponseMessage) or a client-side error (ClientQueryError) such as
    the session being closed or not yet authenticated.
    """
