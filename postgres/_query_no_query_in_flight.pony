use lori = "lori"

trait _QueryNoQueryInFlight is _QueryState
  """
  Default behavior for states where no query is in flight. Query data
  callbacks and result callbacks trigger shutdown — receiving them without
  an active query indicates a protocol anomaly.
  """
  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    li.shutdown(s)

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    li.shutdown(s)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    li.shutdown(s)

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    li.shutdown(s)

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_in_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_out_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_data(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    li.shutdown(s)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    None

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    None

class _QueryNotReady is _QueryNoQueryInFlight
  """
  Server has not yet signaled readiness. This is the initial state after
  authentication, before the first ReadyForQuery arrives.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

class _QueryReady is _QueryNoQueryInFlight
  """
  Server has signaled readiness and can accept a query. If the queue is
  non-empty, `try_run_query` immediately transitions to an in-flight state.

  ReadyForQuery while already ready indicates a protocol anomaly — the
  server only sends ReadyForQuery in response to a query cycle or Sync.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      if li.query_queue.size() > 0 then
        match \exhaustive\ li.query_queue(0)?
        | let qry: _QueuedQuery =>
          match \exhaustive\ qry.query
          | let sq: SimpleQuery =>
            li.query_state = _SimpleQueryInFlight.create()
            s._connection().send(_FrontendMessage.query(sq.string))
          | let pq: PreparedQuery =>
            // Build messages before transitioning state so an encode
            // error in bind() leaves the state machine in _QueryReady.
            let combined =
              try
                let parse =
                  _FrontendMessage.parse(
                    "",
                    pq.string,
                    _ParamEncoder.oids_for(pq.params, li.codec_registry))
                let bind =
                  _FrontendMessage.bind(
                    "", "", pq.params, li.codec_registry)?
                let describe = _FrontendMessage.describe_portal("")
                let execute = _FrontendMessage.execute_msg("", 0)
                let sync = _FrontendMessage.sync()
                recover val
                  let total = parse.size() + bind.size()
                    + describe.size() + execute.size() + sync.size()
                  Array[U8](total)
                    .> copy_from(parse, 0, 0, parse.size())
                    .> copy_from(bind, 0, parse.size(), bind.size())
                    .> copy_from(
                      describe,
                      0,
                      parse.size() + bind.size(),
                      describe.size())
                    .> copy_from(
                      execute,
                      0,
                      parse.size() + bind.size() + describe.size(),
                      execute.size())
                    .> copy_from(
                      sync,
                      0,
                      parse.size() + bind.size() + describe.size()
                        + execute.size(),
                      sync.size())
                end
              else
                qry.receiver.pg_query_failed(s, qry.query, DataError)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
            li.query_state = _ExtendedQueryInFlight.create()
            s._connection().send(combined)
          | let nq: NamedPreparedQuery =>
            let combined =
              try
                let bind =
                  _FrontendMessage.bind(
                    "", nq.name, nq.params, li.codec_registry)?
                let describe = _FrontendMessage.describe_portal("")
                let execute = _FrontendMessage.execute_msg("", 0)
                let sync = _FrontendMessage.sync()
                recover val
                  let total = bind.size() + describe.size()
                    + execute.size() + sync.size()
                  Array[U8](total)
                    .> copy_from(bind, 0, 0, bind.size())
                    .> copy_from(
                      describe, 0, bind.size(), describe.size())
                    .> copy_from(
                      execute,
                      0,
                      bind.size() + describe.size(),
                      execute.size())
                    .> copy_from(
                      sync,
                      0,
                      bind.size() + describe.size() + execute.size(),
                      sync.size())
                end
              else
                qry.receiver.pg_query_failed(s, qry.query, DataError)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
            li.query_state = _ExtendedQueryInFlight.create()
            s._connection().send(combined)
          end
        | let prep: _QueuedPrepare =>
          li.query_state = _PrepareInFlight.create()
          let parse =
            _FrontendMessage.parse(
              prep.name, prep.sql, recover val Array[U32] end)
          let describe = _FrontendMessage.describe_statement(prep.name)
          let sync = _FrontendMessage.sync()
          let combined =
            recover val
              let total = parse.size() + describe.size() + sync.size()
              Array[U8](total)
                .> copy_from(parse, 0, 0, parse.size())
                .> copy_from(
                  describe, 0, parse.size(), describe.size())
                .> copy_from(
                  sync,
                  0,
                  parse.size() + describe.size(),
                  sync.size())
            end
          s._connection().send(consume combined)
        | let cs: _QueuedCloseStatement =>
          li.query_state = _CloseStatementInFlight.create()
          let close = _FrontendMessage.close_statement(cs.name)
          let sync = _FrontendMessage.sync()
          let combined =
            recover val
              let total = close.size() + sync.size()
              Array[U8](total)
                .> copy_from(close, 0, 0, close.size())
                .> copy_from(sync, 0, close.size(), sync.size())
            end
          s._connection().send(consume combined)
        | let ci: _QueuedCopyIn =>
          li.query_state = _CopyInInFlight
          s._connection().send(_FrontendMessage.query(ci.sql))
        | let co: _QueuedCopyOut =>
          li.query_state = _CopyOutInFlight
          s._connection().send(_FrontendMessage.query(co.sql))
        | let sq: _QueuedStreamingQuery =>
          match \exhaustive\ sq.query
          | let pq: PreparedQuery =>
            let combined =
              try
                let parse =
                  _FrontendMessage.parse(
                    "",
                    pq.string,
                    _ParamEncoder.oids_for(
                      pq.params, li.codec_registry))
                let bind =
                  _FrontendMessage.bind(
                    "", "", pq.params, li.codec_registry)?
                let describe = _FrontendMessage.describe_portal("")
                let execute =
                  _FrontendMessage.execute_msg("", sq.window_size)
                let flush_msg = _FrontendMessage.flush()
                recover val
                  let total = parse.size() + bind.size()
                    + describe.size() + execute.size()
                    + flush_msg.size()
                  Array[U8](total)
                    .> copy_from(parse, 0, 0, parse.size())
                    .> copy_from(bind, 0, parse.size(), bind.size())
                    .> copy_from(
                      describe,
                      0,
                      parse.size() + bind.size(),
                      describe.size())
                    .> copy_from(
                      execute,
                      0,
                      parse.size() + bind.size() + describe.size(),
                      execute.size())
                    .> copy_from(
                      flush_msg,
                      0,
                      parse.size() + bind.size() + describe.size()
                        + execute.size(),
                      flush_msg.size())
                end
              else
                sq.receiver.pg_stream_failed(s, sq.query, DataError)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
            li.query_state = _StreamingQueryInFlight.create()
            s._connection().send(combined)
          | let nq: NamedPreparedQuery =>
            let combined =
              try
                let bind =
                  _FrontendMessage.bind(
                    "", nq.name, nq.params, li.codec_registry)?
                let describe = _FrontendMessage.describe_portal("")
                let execute =
                  _FrontendMessage.execute_msg("", sq.window_size)
                let flush_msg = _FrontendMessage.flush()
                recover val
                  let total = bind.size() + describe.size()
                    + execute.size() + flush_msg.size()
                  Array[U8](total)
                    .> copy_from(bind, 0, 0, bind.size())
                    .> copy_from(
                      describe, 0, bind.size(), describe.size())
                    .> copy_from(
                      execute,
                      0,
                      bind.size() + describe.size(),
                      execute.size())
                    .> copy_from(
                      flush_msg,
                      0,
                      bind.size() + describe.size()
                        + execute.size(),
                      flush_msg.size())
                end
              else
                sq.receiver.pg_stream_failed(s, sq.query, DataError)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
            li.query_state = _StreamingQueryInFlight.create()
            s._connection().send(combined)
          end
        | let pl: _QueuedPipeline =>
          if pl.queries.size() == 0 then
            pl.receiver.pg_pipeline_complete(s)
            try
              li.query_queue.shift()?
            else
              _Unreachable()
            end
            try_run_query(s, li)
            return
          end
          let parts = recover iso Array[Array[U8] val] end
          for (qi, query) in pl.queries.pairs() do
            match \exhaustive\ query
            | let pq: PreparedQuery =>
              parts.push(
                _FrontendMessage.parse(
                  "",
                  pq.string,
                  _ParamEncoder.oids_for(
                    pq.params, li.codec_registry)))
              try
                parts.push(
                  _FrontendMessage.bind(
                    "", "", pq.params, li.codec_registry)?)
              else
                var i: USize = 0
                while i < pl.queries.size() do
                  try
                    pl.receiver.pg_pipeline_failed(
                      s, i, pl.queries(i)?, DataError)
                  else
                    _Unreachable()
                  end
                  i = i + 1
                end
                pl.receiver.pg_pipeline_complete(s)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
              parts.push(_FrontendMessage.describe_portal(""))
              parts.push(_FrontendMessage.execute_msg("", 0))
              parts.push(_FrontendMessage.sync())
            | let nq: NamedPreparedQuery =>
              try
                parts.push(
                  _FrontendMessage.bind(
                    "", nq.name, nq.params, li.codec_registry)?)
              else
                var i: USize = 0
                while i < pl.queries.size() do
                  try
                    pl.receiver.pg_pipeline_failed(
                      s, i, pl.queries(i)?, DataError)
                  else
                    _Unreachable()
                  end
                  i = i + 1
                end
                pl.receiver.pg_pipeline_complete(s)
                try li.query_queue.shift()? else _Unreachable() end
                try_run_query(s, li)
                return
              end
              parts.push(_FrontendMessage.describe_portal(""))
              parts.push(_FrontendMessage.execute_msg("", 0))
              parts.push(_FrontendMessage.sync())
            end
          end
          let combined =
            recover val
              let p: Array[Array[U8] val] ref = consume parts
              var total: USize = 0
              for part in p.values() do
                total = total + part.size()
              end
              let buf = Array[U8](total)
              var offset: USize = 0
              for part in p.values() do
                buf.copy_from(part, 0, offset, part.size())
                offset = offset + part.size()
              end
              buf
            end
          li.query_state = _PipelineInFlight.create()
          s._connection().send(consume combined)
        end

        // Set statement timeout timer if configured on the dispatched item.
        // The queue item is still at index 0 — dequeuing happens in each
        // in-flight state's on_ready_for_query. COUPLING: the per-variant
        // timeout extraction must stay in sync with
        // `_SessionLoggedIn.on_timer_failure`, which rearms this timer on
        // ASIO subscription failure.
        let timeout =
          match \exhaustive\ li.query_queue(0)?
          | let qry: _QueuedQuery => qry.statement_timeout
          | let prep: _QueuedPrepare => prep.statement_timeout
          | let _: _QueuedCloseStatement => None
          | let ci: _QueuedCopyIn => ci.statement_timeout
          | let co: _QueuedCopyOut => co.statement_timeout
          | let sq: _QueuedStreamingQuery => sq.statement_timeout
          | let pl: _QueuedPipeline => pl.statement_timeout
          end
        match timeout
        | let d: lori.TimerDuration =>
          match \exhaustive\ s._connection().set_timer(d)
          | let t: lori.TimerToken => li.statement_timer = t
          | let _: lori.SetTimerError => None
          end
        end
      end
    else
      _Unreachable()
    end

