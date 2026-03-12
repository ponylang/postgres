## Add query pipelining

Query pipelining sends multiple queries to the server in a single TCP write and processes all responses in order, reducing round-trip latency from N round trips to 1. Each query has its own error isolation boundary — if one fails, subsequent queries continue executing.

A new `PipelineReceiver` interface provides three callbacks: `pg_pipeline_result` delivers individual query results with their pipeline index, `pg_pipeline_failed` delivers individual failures, and `pg_pipeline_complete` signals all queries have been processed.

```pony
// Pipeline 3 queries in a single call
let queries = recover val
  [as (PreparedQuery | NamedPreparedQuery):
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as (String | None): "1"] end)
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as (String | None): "2"] end)
    PreparedQuery("SELECT * FROM users WHERE id = $1",
      recover val [as (String | None): "3"] end)
  ]
end
session.pipeline(queries, my_receiver)

// In the receiver:
be pg_pipeline_result(session: Session, index: USize, result: Result) =>
  // Handle result for query at `index`

be pg_pipeline_complete(session: Session) =>
  // All queries processed
```

Only `PreparedQuery` and `NamedPreparedQuery` are supported — pipelining uses the extended query protocol.
