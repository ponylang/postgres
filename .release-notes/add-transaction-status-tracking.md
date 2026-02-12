## Add transaction status tracking

Every PostgreSQL `ReadyForQuery` message includes a transaction status byte. The new `pg_transaction_status` callback on `SessionStatusNotify` exposes this as a `TransactionStatus` union type, letting you track whether the session is idle, inside a transaction block, or in a failed transaction state.

```pony
actor Client is (SessionStatusNotify & ResultReceiver)
  be pg_transaction_status(session: Session, status: TransactionStatus) =>
    match status
    | TransactionIdle => // not in a transaction
    | TransactionInBlock => // inside BEGIN...COMMIT/ROLLBACK
    | TransactionFailed => // error occurred, must ROLLBACK
    end
```

The callback fires after every query cycle completes, including the initial ready signal after authentication. Existing code is unaffected — the callback has a default no-op body.
