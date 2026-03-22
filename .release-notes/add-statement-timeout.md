## Add statement timeout

All query operations (`execute`, `prepare`, `copy_in`, `copy_out`, `stream`, `pipeline`) now accept an optional `statement_timeout` parameter. When provided, the driver starts a one-shot timer and sends a CancelRequest if the operation does not complete within the given duration. The cancelled query fails with SQLSTATE 57014 (`query_canceled`), the same as a manual `cancel()` call.

```pony
match lori.MakeTimerDuration(5000) // 5 seconds
| let d: lori.TimerDuration =>
  session.execute(query, receiver where statement_timeout = d)
end
```

The timeout covers the entire operation: for streaming queries, from the initial Execute to the final ReadyForQuery; for pipelines, from the first query to the last. The timer is automatically cancelled when the operation completes normally.
