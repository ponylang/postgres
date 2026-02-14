## Add ParameterStatus tracking

PostgreSQL sends ParameterStatus messages during connection startup to report runtime parameter values (server_version, client_encoding, standard_conforming_strings, etc.) and again whenever a `SET` command changes a reporting parameter. Previously, the driver silently discarded these messages.

A new `pg_parameter_status` callback on `SessionStatusNotify` delivers each parameter as a `ParameterStatus` value with `name` and `value` fields:

```pony
actor MyNotify is SessionStatusNotify
  be pg_parameter_status(session: Session, status: ParameterStatus) =>
    _env.out.print(status.name + " = " + status.value)
```

The callback has a default no-op implementation, so existing code is unaffected.
