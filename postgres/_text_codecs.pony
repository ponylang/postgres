use "constrained_types"

primitive _BoolTextCodec is Codec
  """
  Text codec for PostgreSQL `bool` (OID 16).
  Decodes "t" as true, anything else as false.
  Encodes as "t" or "f".
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: Bool =>
      if v then recover val "t".array() end
      else recover val "f".array() end
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data).at("t")

primitive _ByteaTextCodec is Codec
  """
  Text codec for PostgreSQL `bytea` (OID 17).
  Decodes hex-format bytea (`\xDEADBEEF` -> bytes).
  Encodes as hex-format bytea.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: Array[U8] val =>
      recover val
        let hex = Array[U8](2 + (v.size() * 2))
        hex.push('\\')
        hex.push('x')
        for b in v.values() do
          hex.push(_to_hex_digit((b >> 4) and 0x0F))
          hex.push(_to_hex_digit(b and 0x0F))
        end
        hex
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    if (s.size() < 2) or (s(0)? != '\\') or (s(1)? != 'x') then error end
    let hex_len = s.size() - 2
    if (hex_len % 2) != 0 then error end
    recover val
      let result = Array[U8](hex_len / 2)
      var i: USize = 2
      while i < s.size() do
        let hi = _hex_digit(s(i)?)?
        let lo = _hex_digit(s(i + 1)?)?
        result.push((hi * 16) + lo)
        i = i + 2
      end
      result
    end

  fun _hex_digit(c: U8): U8 ? =>
    if (c >= '0') and (c <= '9') then c - '0'
    elseif (c >= 'a') and (c <= 'f') then (c - 'a') + 10
    elseif (c >= 'A') and (c <= 'F') then (c - 'A') + 10
    else error
    end

  fun _to_hex_digit(nibble: U8): U8 =>
    if nibble < 10 then nibble + '0'
    else (nibble - 10) + 'a'
    end

primitive _Int2TextCodec is Codec
  """
  Text codec for PostgreSQL `int2` (OID 21).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I16 => recover val v.string().array() end
    else error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    String.from_array(data).i16()?

primitive _Int4TextCodec is Codec
  """
  Text codec for PostgreSQL `int4` (OID 23).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I32 => recover val v.string().array() end
    else error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    String.from_array(data).i32()?

primitive _Int8TextCodec is Codec
  """
  Text codec for PostgreSQL `int8` (OID 20).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I64 => recover val v.string().array() end
    else error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    String.from_array(data).i64()?

primitive _Float4TextCodec is Codec
  """
  Text codec for PostgreSQL `float4` (OID 700).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: F32 => recover val v.string().array() end
    else error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    String.from_array(data).f32()?

primitive _Float8TextCodec is Codec
  """
  Text codec for PostgreSQL `float8` (OID 701).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: F64 => recover val v.string().array() end
    else error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    String.from_array(data).f64()?

primitive _TextPassthroughTextCodec is Codec
  """
  Text codec for PostgreSQL text-like types. Text format is already a readable
  String, so decode is a simple passthrough. Registered for OIDs: char (18),
  name (19), text (25), json (114), xml (142), bpchar (1042), varchar (1043).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)

primitive _OidTextCodec is Codec
  """
  Text codec for PostgreSQL `oid` (OID 26).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)

primitive _NumericTextCodec is Codec
  """
  Text codec for PostgreSQL `numeric` (OID 1700).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)

primitive _UuidTextCodec is Codec
  """
  Text codec for PostgreSQL `uuid` (OID 2950).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)

primitive _JsonbTextCodec is Codec
  """
  Text codec for PostgreSQL `jsonb` (OID 3802).
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)

primitive _DateTextCodec is Codec
  """
  Text codec for PostgreSQL `date` (OID 1082).
  Parses `YYYY-MM-DD`, `infinity`, `-infinity`.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgDate => v.string().array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    if s == "infinity" then return PgDate(I32.max_value()) end
    if s == "-infinity" then return PgDate(I32.min_value()) end
    (let year, let month, let day) = _parse_date_parts(s)?
    let julian = _gregorian_to_julian(year, month, day)
    PgDate((julian - _TemporalFormat.pg_epoch_jdn()).i32())

  fun _parse_date_parts(s: String): (I32, I32, I32) ? =>
    """
    Parse `YYYY-MM-DD` (with optional leading `-` for negative years) into
    (year, month, day). Used by both `_DateTextCodec` and
    `_TimestampTextCodec`.
    """
    let parts = s.split("-")
    let negative_year = s.at("-", 0) and (parts.size() > 3)
    let year: I32 = if negative_year then
      -parts(1)?.i32()?
    else
      parts(0)?.i32()?
    end
    let month_idx: USize = if negative_year then 2 else 1 end
    let day_idx: USize = if negative_year then 3 else 2 end
    let month = parts(month_idx)?.i32()?
    let day = parts(day_idx)?.i32()?
    (year, month, day)

  fun _gregorian_to_julian(year: I32, month: I32, day: I32): I64 =>
    """
    Convert Gregorian date to Julian Day Number. Inverse of
    `_TemporalFormat._julian_to_gregorian`.
    """
    let a = (14 - month).i64() / 12
    let y = (year.i64() + 4800) - a
    let m = (month.i64() + (12 * a)) - 3
    let j = day.i64() + (((153 * m) + 2) / 5) + (365 * y) + (y / 4)
    ((j - (y / 100)) + (y / 400)) - 32045

primitive _TimeTextCodec is Codec
  """
  Text codec for PostgreSQL `time` (OID 1083).
  Parses `HH:MM:SS` with optional `.ffffff` fractional seconds.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgTime => v.string().array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    (let hours, let minutes, let seconds, let frac) = _parse_time(s)?
    let us: I64 = (hours * 3_600_000_000) + (minutes * 60_000_000)
      + (seconds * 1_000_000) + frac
    PgTime(MakePgTimeMicroseconds(us) as PgTimeMicroseconds)

  fun _parse_time(s: String): (I64, I64, I64, I64) ? =>
    """
    Parse HH:MM:SS[.ffffff] and return (hours, minutes, seconds, frac_us).
    """
    let time_parts = s.split(":")
    let hours = time_parts(0)?.i64()?
    let minutes = time_parts(1)?.i64()?
    let sec_str = time_parts(2)?
    let sec_parts = sec_str.split(".")
    let seconds = sec_parts(0)?.i64()?
    let frac: I64 = if sec_parts.size() > 1 then
      _parse_fractional(sec_parts(1)?)?
    else
      0
    end
    (hours, minutes, seconds, frac)

  fun _parse_fractional(s: String): I64 ? =>
    """
    Parse fractional seconds string (up to 6 digits) into microseconds.
    """
    var result: I64 = 0
    var i: USize = 0
    while (i < s.size()) and (i < 6) do
      let ch = s(i)?
      if (ch < '0') or (ch > '9') then error end
      result = result.mul_partial(10)?.add_partial((ch - '0').i64())?
      i = i + 1
    end
    // Pad with zeros if fewer than 6 digits
    while i < 6 do
      result = result * 10
      i = i + 1
    end
    result

primitive _TimestampTextCodec is Codec
  """
  Text codec for PostgreSQL `timestamp` (OID 1114).
  Parses `YYYY-MM-DD HH:MM:SS[.ffffff]`, `infinity`, `-infinity`.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgTimestamp => v.string().array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    if s == "infinity" then return PgTimestamp(I64.max_value()) end
    if s == "-infinity" then return PgTimestamp(I64.min_value()) end
    _parse_timestamp(s)?

  fun _parse_timestamp(s: String): PgTimestamp ? =>
    // Split on space to get date and time parts
    let space_parts = s.split(" ")
    let date_str = space_parts(0)?
    let time_str = space_parts(1)?

    // Parse date and time
    (let year, let month, let day) =
      _DateTextCodec._parse_date_parts(date_str)?
    (let hours, let minutes, let seconds, let frac) =
      _TimeTextCodec._parse_time(time_str)?

    // Convert to microseconds since 2000-01-01
    let julian = _DateTextCodec._gregorian_to_julian(year, month, day)
    let day_offset = julian - _TemporalFormat.pg_epoch_jdn()
    let us_per_day: I64 = 86_400_000_000
    let time_us = (hours * 3_600_000_000) + (minutes * 60_000_000)
      + (seconds * 1_000_000) + frac
    PgTimestamp((day_offset * us_per_day) + time_us)

primitive _TimestamptzTextCodec is Codec
  """
  Text codec for PostgreSQL `timestamptz` (OID 1184).
  Parses `YYYY-MM-DD HH:MM:SS[.ffffff]+TZ`, `infinity`, `-infinity`.

  Strips the timezone suffix and parses the remaining timestamp as-is. This
  means the resulting `PgTimestamp` microseconds represent the session-local
  time, not UTC. By contrast, binary-format `timestamptz` (used by
  `PreparedQuery`) stores UTC microseconds. Users whose PostgreSQL session
  timezone is not UTC will get different microsecond values for the same row
  depending on query path.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgTimestamp => v.string().array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    if s == "infinity" then return PgTimestamp(I64.max_value()) end
    if s == "-infinity" then return PgTimestamp(I64.min_value()) end

    // Strip timezone suffix. PostgreSQL text format appends +HH, -HH,
    // +HH:MM, or -HH:MM. Find the last +/- after the time portion.
    // The time starts after the first space. Look for +/- after position 19
    // (minimum "YYYY-MM-DD HH:MM:SS" = 19 chars).
    var tz_pos: ISize = -1
    var i: USize = 19
    while i < s.size() do
      try
        let c = s(i)?
        if (c == '+') or (c == '-') then
          tz_pos = i.isize()
        end
      end
      i = i + 1
    end

    let ts_str = if tz_pos > 0 then
      s.substring(0, tz_pos)
    else
      s
    end

    _TimestampTextCodec._parse_timestamp(consume ts_str)?

primitive _IntervalTextCodec is Codec
  """
  Text codec for PostgreSQL `interval` (OID 1186).
  Parses PostgreSQL `postgres` output style:
  `1 year 2 mons 3 days 04:05:06.789`
  Components are optional; time part is always last.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgInterval => v.string().array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    let s = String.from_array(data)
    var total_months: I32 = 0
    var total_days: I32 = 0
    var total_us: I64 = 0

    // Split into tokens
    let tokens = s.split(" ")
    var ti: USize = 0
    while ti < tokens.size() do
      let tok = tokens(ti)?
      if tok.size() == 0 then
        ti = ti + 1
        continue
      end

      // Check if this token contains ':' — it's the time part
      if tok.contains(":") then
        // Parse [-]HH:MM:SS[.ffffff]
        let negative = tok.at("-")
        let time_str = if negative then
          tok.substring(1)
        else
          tok
        end
        (let h, let m, let sec, let frac) =
          _TimeTextCodec._parse_time(consume time_str)?
        let us = (h * 3_600_000_000) + (m * 60_000_000)
          + (sec * 1_000_000) + frac
        total_us = if negative then -us else us end
        ti = ti + 1
      else
        // Number + unit pair
        let num = tok.i64()?
        ti = ti + 1
        if ti >= tokens.size() then error end
        let unit = tokens(ti)?
        if unit.at("year") then
          total_months = total_months + (num.i32() * 12)
        elseif unit.at("mon") then
          total_months = total_months + num.i32()
        elseif unit.at("day") then
          total_days = total_days + num.i32()
        end
        ti = ti + 1
      end
    end

    PgInterval(total_us, total_days, total_months)
