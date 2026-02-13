## Surface NoticeResponse messages via pg_notice callback

PostgreSQL sends NoticeResponse messages for non-fatal informational feedback — for example, "table does not exist, skipping" when you run `DROP TABLE IF EXISTS` on a nonexistent table, or `RAISE NOTICE` output from PL/pgSQL functions. Previously, the driver silently discarded these messages.

A new `pg_notice` callback on `SessionStatusNotify` delivers notices as `NoticeResponseMessage` values with the full set of PostgreSQL notice fields (severity, code, message, detail, hint, etc.):

```pony
actor MyNotify is SessionStatusNotify
  be pg_notice(session: Session, notice: NoticeResponseMessage) =>
    _env.out.print("[" + notice.severity + "] " + notice.code + ": "
      + notice.message)
```

The callback has a default no-op implementation, so existing code is unaffected.
