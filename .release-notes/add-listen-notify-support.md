## Add LISTEN/NOTIFY support

The driver now delivers PostgreSQL asynchronous notifications via a new `pg_notification` callback on `SessionStatusNotify`. Subscribe to a channel with `LISTEN` and receive notifications as they arrive from the server.

New types:

- `Notification` — a val class with `channel: String`, `payload: String`, and `pid: I32` fields
- `pg_notification(session, notification)` — a new behavior on `SessionStatusNotify` with a default no-op body (existing code is unaffected)

Usage:

```pony
actor MyClient is (SessionStatusNotify & ResultReceiver)
  be pg_session_authenticated(session: Session) =>
    session.execute(SimpleQuery("LISTEN my_channel"), this)

  be pg_notification(session: Session, notification: Notification) =>
    env.out.print("Got: " + notification.channel + " -> " + notification.payload)
```
