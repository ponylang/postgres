use lori = "lori"

interface _QueryState
  """
  Callbacks for query-related protocol messages plus an entry point to
  attempt starting the next queued query.
  """
  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref)

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)

  fun ref on_empty_query_response(s: Session ref, li: _SessionLoggedIn ref)

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)

  fun ref on_copy_in_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)

  fun ref on_copy_out_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)

  fun ref on_copy_data(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called when a portal is suspended during streaming (more rows available).
    """

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref)

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref)

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called from `_SessionLoggedIn.on_protocol_violation`. In-flight states
    deliver a `ProtocolViolation` failure to their receiver and set their
    internal `_error` flag so the subsequent `drain_in_flight` skips the
    already-notified item. States with no query in flight are a no-op.
    """

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref)
    """
    Called from `_SessionLoggedIn.on_closed` when the server closes the TCP
    connection. In-flight states deliver a `SessionClosed` failure to their
    receiver and set their internal `_error` flag so the subsequent
    `drain_in_flight` skips the already-notified item. States with no query
    in flight are a no-op.
    """

class _SimpleQueryInFlight is _QueryState
  """
  Simple query protocol in progress. Owns the per-query accumulation data
  which is created fresh for each query and destroyed when the state
  transitions out.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows =
          _data_rows =
            recover iso
              Array[Array[(Array[U8] val | None)] val].create()
            end
        let rd =
          _row_description = None

        match \exhaustive\ rd
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object =
              _RowsBuilder(consume rows, desc, li.codec_registry)?
            qry.receiver.pg_query_result(
              s, ResultSet(qry.query, rows_object, msg.id))
          else
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          end
        | None =>
          if rows.size() > 0 then
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          else
            qry.receiver.pg_query_result(
              s, RowModifying(qry.query, msg.id, msg.value))
          end
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows =
          _data_rows =
            recover iso Array[Array[(Array[U8] val | None)] val] end
        let rd =
          _row_description = None

        if (rows.size() > 0) or (rd isnt None) then
          qry.receiver.pg_query_failed(s, qry.query, DataError)
        else
          qry.receiver.pg_query_result(s, SimpleResult(qry.query))
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _ExtendedQueryInFlight is _QueryState
  """
  Extended query protocol in progress. Owns the per-query accumulation data
  which is created fresh for each query and destroyed when the state
  transitions out.

  The data accumulation and result delivery logic is identical to
  `_SimpleQueryInFlight`. The duplication exists because Pony traits cannot
  have `iso` fields, so the shared `_data_rows` and `_row_description` state
  cannot be factored into a trait.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows =
          _data_rows =
            recover iso
              Array[Array[(Array[U8] val | None)] val].create()
            end
        let rd =
          _row_description = None

        match \exhaustive\ rd
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object =
              _RowsBuilder(consume rows, desc, li.codec_registry)?
            qry.receiver.pg_query_result(
              s, ResultSet(qry.query, rows_object, msg.id))
          else
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          end
        | None =>
          if rows.size() > 0 then
            qry.receiver.pg_query_failed(s, qry.query, DataError)
          else
            qry.receiver.pg_query_result(
              s, RowModifying(qry.query, msg.id, msg.value))
          end
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        let rows =
          _data_rows =
            recover iso Array[Array[(Array[U8] val | None)] val] end
        let rd =
          _row_description = None

        if (rows.size() > 0) or (rd isnt None) then
          qry.receiver.pg_query_failed(s, qry.query, DataError)
        else
          qry.receiver.pg_query_result(s, SimpleResult(qry.query))
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      match li.query_queue(0)?
      | let qry: _QueuedQuery =>
        _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
        _row_description = None
        qry.receiver.pg_query_failed(s, qry.query, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let qry: _QueuedQuery =>
          qry.receiver.pg_query_failed(s, qry.query, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _PrepareInFlight is _QueryState
  """
  Prepare (named statement) protocol in progress. Expects ParseComplete,
  ParameterDescription, RowDescription (or NoData), then ReadyForQuery.
  On error, ErrorResponse arrives before ReadyForQuery.
  """
  var _error: Bool = false

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    // Received from Describe(statement) — not cached in this version.
    None

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      match li.query_queue(0)?
      | let prep: _QueuedPrepare =>
        prep.receiver.pg_prepare_failed(s, prep.name, msg)
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_statement_prepared(s, prep.name)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    li.shutdown(s)

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    li.shutdown(s)

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, ProtocolViolation)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        match li.query_queue(0)?
        | let prep: _QueuedPrepare =>
          prep.receiver.pg_prepare_failed(s, prep.name, SessionClosed)
        else
          _Unreachable()
        end
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _CloseStatementInFlight is _QueryState
  """
  Close (named statement) protocol in progress. Expects CloseComplete then
  ReadyForQuery. Fire-and-forget: errors are silently consumed.
  """
  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    // Fire-and-forget: ReadyForQuery still arrives to dequeue.
    None

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    li.shutdown(s)

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    // Fire-and-forget: no receiver to notify; drain_in_flight shifts
    // unconditionally in this state.
    None

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    // Fire-and-forget: no receiver to notify.
    None

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _CopyInInFlight is _QueryState
  """
  COPY ... FROM STDIN operation in progress. Uses a pull-based data flow:
  `pg_copy_ready` is called on the receiver to request each chunk. The receiver
  responds by calling `send_copy_data`, `finish_copy`, or `abort_copy` on the
  session.
  """
  var _complete_count: USize = 0
  var _error: Bool = false

  fun ref on_copy_in_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_ready(s)
    else
      _Unreachable()
    end

  fun ref send_copy_data(
    s: Session ref,
    li: _SessionLoggedIn ref,
    data: Array[U8] val)
  =>
    s._connection().send(_FrontendMessage.copy_data(data))
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_ready(s)
    else
      _Unreachable()
    end

  fun ref finish_copy(s: Session ref) =>
    s._connection().send(_FrontendMessage.copy_done())

  fun ref abort_copy(s: Session ref, reason: String) =>
    s._connection().send(_FrontendMessage.copy_fail(reason))

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(
        s, msg)
    else
      _Unreachable()
    end

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    _complete_count = msg.value

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_complete(
          s, _complete_count)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(
          s, ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(
          s, SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyIn).receiver.pg_copy_failed(
          s, SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _CopyOutInFlight is _QueryState
  """
  COPY ... TO STDOUT operation in progress. The server pushes data via
  CopyData messages. Each chunk is delivered to the receiver's
  `pg_copy_data` callback. The operation completes with CopyDone followed
  by CommandComplete and ReadyForQuery.
  """
  var _complete_count: USize = 0
  var _error: Bool = false

  fun ref on_copy_out_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyOutResponseMessage)
  =>
    // Server is ready to send data. Nothing to do — data will arrive as
    // CopyData messages.
    None

  fun ref on_copy_data(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyDataMessage)
  =>
    try
      (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_data(
        s, msg.data)
    else
      _Unreachable()
    end

  fun ref on_copy_done(s: Session ref, li: _SessionLoggedIn ref) =>
    // Data stream complete. Wait for CommandComplete + ReadyForQuery.
    None

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(
        s, msg)
    else
      _Unreachable()
    end

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    _complete_count = msg.value

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_complete(
          s, _complete_count)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

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

  fun ref on_copy_in_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CopyInResponseMessage)
  =>
    li.shutdown(s)

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    li.shutdown(s)

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(
          s, ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(
          s, SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        (li.query_queue(0)? as _QueuedCopyOut).receiver.pg_copy_failed(
          s, SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _StreamingQueryInFlight is _QueryState
  """
  Streaming query in progress. Delivers rows in windowed batches via
  `StreamingResultReceiver`. Uses Execute(max_rows > 0) + Flush to keep the
  portal alive between batches, with Sync sent only on completion or error
  to trigger ReadyForQuery. `_completing` guards against `fetch_more` and
  `close_stream` sending messages after `on_command_complete` has already
  sent Sync — the receiver may call `fetch_more()` in response to the
  final `pg_stream_batch` before `ReadyForQuery` arrives.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false
  var _completing: Bool = false

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_portal_suspended(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let rows =
        _data_rows =
          recover iso
            Array[Array[(Array[U8] val | None)] val].create()
          end
      match _row_description
      | let desc: Array[(String, U32, U16)] val =>
        try
          let rows_object =
            _RowsBuilder(consume rows, desc, li.codec_registry)?
          sq.receiver.pg_stream_batch(s, rows_object)
        else
          _error = true
          _row_description = None
          sq.receiver.pg_stream_failed(s, sq.query, DataError)
          s._connection().send(_FrontendMessage.sync())
        end
      else
        _Unreachable()
      end
    else
      _Unreachable()
    end

  fun ref fetch_more(s: Session ref, li: _SessionLoggedIn ref) =>
    // After CommandComplete or ErrorResponse, the portal is destroyed by
    // Sync. The receiver may still call fetch_more() in response to the
    // final pg_stream_batch before ReadyForQuery arrives — silently ignore.
    if _completing or _error then return end
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let execute = _FrontendMessage.execute_msg("", sq.window_size)
      let flush_msg = _FrontendMessage.flush()
      let combined =
        recover val
          let total = execute.size() + flush_msg.size()
          Array[U8](total)
            .> copy_from(execute, 0, 0, execute.size())
            .> copy_from(
              flush_msg, 0, execute.size(), flush_msg.size())
        end
      s._connection().send(consume combined)
    else
      _Unreachable()
    end

  fun ref close_stream(s: Session ref) =>
    if (not _error) and (not _completing) then
      s._connection().send(_FrontendMessage.sync())
    end

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    // Final batch — deliver any remaining accumulated rows.
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      let rows =
        _data_rows =
          recover iso
            Array[Array[(Array[U8] val | None)] val].create()
          end
      if rows.size() > 0 then
        match _row_description
        | let desc: Array[(String, U32, U16)] val =>
          try
            let rows_object =
              _RowsBuilder(consume rows, desc, li.codec_registry)?
            sq.receiver.pg_stream_batch(s, rows_object)
          else
            _error = true
            _row_description = None
            sq.receiver.pg_stream_failed(s, sq.query, DataError)
          end
        else
          _Unreachable()
        end
      end
    else
      _Unreachable()
    end
    // Send Sync to trigger ReadyForQuery and destroy the portal.
    // _completing prevents close_stream() from sending a duplicate Sync if it
    // arrives between this point and ReadyForQuery.
    _completing = true
    s._connection().send(_FrontendMessage.sync())

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_complete(s)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end
    li.cancel_statement_timer(s)
    li.query_state = _QueryReady
    li.query_state.try_run_query(s, li)

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      let sq = li.query_queue(0)? as _QueuedStreamingQuery
      _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
      _row_description = None
      sq.receiver.pg_stream_failed(s, sq.query, msg)
    else
      _Unreachable()
    end
    // Sync is required because streaming uses Flush (not Sync) to keep the
    // portal alive. Without a pending Sync, the server waits indefinitely
    // after ErrorResponse, deadlocking the session.
    s._connection().send(_FrontendMessage.sync())

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    if not _error then
      try
        let sq = li.query_queue(0)? as _QueuedStreamingQuery
        sq.receiver.pg_stream_failed(s, sq.query, SessionClosed)
      else
        _Unreachable()
      end
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

class _PipelineInFlight is _QueryState
  """
  Pipeline execution in progress. Processes N extended query cycles, one per
  pipelined query. Each cycle ends with its own Sync/ReadyForQuery. Per-query
  accumulation data is reset between cycles. `_current_index` tracks which
  query in the pipeline is currently being processed.

  Error isolation: each query has its own Sync boundary. If query 2 fails,
  the server skips to Sync2 and continues with query 3. The `_error` flag
  is per-query, reset on each ReadyForQuery.
  """
  var _data_rows: Array[Array[(Array[U8] val | None)] val] iso
  var _row_description: (Array[(String, U32, U16)] val | None)
  var _error: Bool = false
  var _current_index: USize = 0

  new create() =>
    _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
    _row_description = None

  fun ref try_run_query(s: Session ref, li: _SessionLoggedIn ref) => None

  fun ref on_data_row(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _DataRowMessage)
  =>
    _data_rows.push(msg.columns)

  fun ref on_row_description(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _RowDescriptionMessage)
  =>
    _row_description = msg.columns

  fun ref on_command_complete(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: _CommandCompleteMessage)
  =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      let rows =
        _data_rows =
          recover iso
            Array[Array[(Array[U8] val | None)] val].create()
          end
      let rd = _row_description = None

      match \exhaustive\ rd
      | let desc: Array[(String, U32, U16)] val =>
        try
          let rows_object =
            _RowsBuilder(consume rows, desc, li.codec_registry)?
          pl.receiver.pg_pipeline_result(
            s,
            _current_index,
            ResultSet(
              pl.queries(_current_index)?,
              rows_object,
              msg.id))
        else
          pl.receiver.pg_pipeline_failed(
            s,
            _current_index,
            pl.queries(_current_index)?,
            DataError)
        end
      | None =>
        if rows.size() > 0 then
          pl.receiver.pg_pipeline_failed(
            s,
            _current_index,
            pl.queries(_current_index)?,
            DataError)
        else
          pl.receiver.pg_pipeline_result(
            s,
            _current_index,
            RowModifying(
              pl.queries(_current_index)?,
              msg.id,
              msg.value))
        end
      end
    else
      _Unreachable()
    end

  fun ref on_empty_query_response(
    s: Session ref,
    li: _SessionLoggedIn ref)
  =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      let rows =
        _data_rows =
          recover iso Array[Array[(Array[U8] val | None)] val] end
      let rd = _row_description = None

      if (rows.size() > 0) or (rd isnt None) then
        pl.receiver.pg_pipeline_failed(
          s,
          _current_index,
          pl.queries(_current_index)?,
          DataError)
      else
        pl.receiver.pg_pipeline_result(
          s,
          _current_index,
          SimpleResult(pl.queries(_current_index)?))
      end
    else
      _Unreachable()
    end

  fun ref on_error_response(
    s: Session ref,
    li: _SessionLoggedIn ref,
    msg: ErrorResponseMessage)
  =>
    _error = true
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      _data_rows = recover iso Array[Array[(Array[U8] val | None)] val] end
      _row_description = None
      pl.receiver.pg_pipeline_failed(
        s,
        _current_index,
        pl.queries(_current_index)?,
        msg)
    else
      _Unreachable()
    end

  fun ref on_ready_for_query(s: Session ref, li: _SessionLoggedIn ref) =>
    _current_index = _current_index + 1
    _error = false
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      if _current_index >= pl.queries.size() then
        // All queries processed
        pl.receiver.pg_pipeline_complete(s)
        li.query_queue.shift()?
        li.cancel_statement_timer(s)
        li.query_state = _QueryReady
        li.query_state.try_run_query(s, li)
      end
      // Otherwise, continue processing the next query cycle
    else
      _Unreachable()
    end

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

  fun ref on_protocol_violation(s: Session ref, li: _SessionLoggedIn ref) =>
    // Only the current query directly observed the violation. The remaining
    // queries are casualties of session closure and are notified with
    // SessionClosed by the subsequent drain_in_flight path.
    if not _error then
      try
        let pl = li.query_queue(0)? as _QueuedPipeline
        pl.receiver.pg_pipeline_failed(
          s,
          _current_index,
          pl.queries(_current_index)?,
          ProtocolViolation)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref on_closed(s: Session ref, li: _SessionLoggedIn ref) =>
    // Only notify the current query. Remaining queries are delivered
    // SessionClosed by the subsequent drain_in_flight path.
    if not _error then
      try
        let pl = li.query_queue(0)? as _QueuedPipeline
        pl.receiver.pg_pipeline_failed(
          s,
          _current_index,
          pl.queries(_current_index)?,
          SessionClosed)
      else
        _Unreachable()
      end
      _error = true
    end

  fun ref drain_in_flight(s: Session ref, li: _SessionLoggedIn ref) =>
    try
      let pl = li.query_queue(0)? as _QueuedPipeline
      // Notify current query if not already error-notified
      if not _error then
        pl.receiver.pg_pipeline_failed(
          s,
          _current_index,
          pl.queries(_current_index)?,
          SessionClosed)
      end
      // Notify remaining queries
      var i = _current_index + 1
      while i < pl.queries.size() do
        pl.receiver.pg_pipeline_failed(
          s, i, pl.queries(i)?, SessionClosed)
        i = i + 1
      end
      pl.receiver.pg_pipeline_complete(s)
    else
      _Unreachable()
    end
    try
      li.query_queue.shift()?
    else
      _Unreachable()
    end

