primitive TransactionIdle
  """
  The session is idle, not in a transaction block. Each statement is
  auto-committed.
  """

primitive TransactionInBlock
  """
  The session is inside a transaction block (after BEGIN, before
  COMMIT/ROLLBACK).
  """

primitive TransactionFailed
  """
  The session is in a failed transaction block. All statements will be
  rejected by the server until a ROLLBACK is issued.
  """

type TransactionStatus is
  (TransactionIdle | TransactionInBlock | TransactionFailed)
