## Fix ReadyForQuery queue stall with explicit transactions

Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) caused the query queue to permanently stall. Any query following `BEGIN` would never execute because the driver incorrectly treated the server's "in transaction" status as "not ready for the next command."

Transactions now work as expected:

```pony
be pg_session_authenticated(session: Session) =>
  session.execute(SimpleQuery("BEGIN"), receiver)
  session.execute(SimpleQuery("INSERT INTO t (col) VALUES ('x')"), receiver)
  session.execute(SimpleQuery("COMMIT"), receiver)
```
