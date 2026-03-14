use "constrained_types"

class val PgTimestamp is Equatable[PgTimestamp]
  """
  A PostgreSQL `timestamp` or `timestamptz` value. Stores microseconds since
  the PostgreSQL epoch (2000-01-01 00:00:00). Special values
  `I64.max_value()` and `I64.min_value()` represent positive and negative
  infinity respectively.

  For `timestamptz` columns, the interpretation depends on the query format:
  binary-format results (`PreparedQuery`) store UTC microseconds; text-format
  results (`SimpleQuery`) store session-local time with the timezone suffix
  stripped. When sent as a parameter, `PgTimestamp` encodes as OID 1114
  (`timestamp without time zone`); the server applies session-timezone
  conversion when inserting into a `timestamptz` column.
  """
  let microseconds: I64

  new val create(microseconds': I64) =>
    microseconds = microseconds'

  fun eq(that: box->PgTimestamp): Bool =>
    microseconds == that.microseconds

  fun ne(that: box->PgTimestamp): Bool =>
    not eq(that)

  fun string(): String iso^ =>
    if microseconds == I64.max_value() then
      "infinity".clone()
    elseif microseconds == I64.min_value() then
      "-infinity".clone()
    else
      // Decompose microseconds since 2000-01-01 into date and time components
      var remaining = microseconds
      let us_per_second: I64 = 1_000_000
      let us_per_minute: I64 = us_per_second * 60
      let us_per_hour: I64 = us_per_minute * 60
      let us_per_day: I64 = us_per_hour * 24

      // Split into days and time-of-day microseconds
      var day_offset = remaining / us_per_day
      var time_us = remaining % us_per_day
      if time_us < 0 then
        day_offset = day_offset - 1
        time_us = time_us + us_per_day
      end

      let hour = time_us / us_per_hour
      time_us = time_us % us_per_hour
      let minute = time_us / us_per_minute
      time_us = time_us % us_per_minute
      let second = time_us / us_per_second
      let frac = time_us % us_per_second

      (let year, let month, let day) =
        _TemporalFormat.days_to_date(day_offset.i32())

      recover iso
        let s = String
        _TemporalFormat.append_date(s, year, month, day)
        s.append(" ")
        _TemporalFormat.append_two_digits(s, hour)
        s.append(":")
        _TemporalFormat.append_two_digits(s, minute)
        s.append(":")
        _TemporalFormat.append_two_digits(s, second)
        if frac != 0 then
          s.append(".")
          _TemporalFormat.append_fractional(s, frac)
        end
        s
      end
    end

primitive PgTimeValidator is Validator[I64]
  """
  Validates that an I64 microseconds value is in the valid range for a
  PostgreSQL `time` value: [0, 86,400,000,000).
  """
  fun apply(us: I64): ValidationResult =>
    recover val
      if (us < 0) or (us >= 86_400_000_000) then
        ValidationFailure(
          "PgTime microseconds must be in [0, 86_400_000_000)")
      else
        ValidationSuccess
      end
    end

// Validated microseconds value for constructing a PgTime.
type PgTimeMicroseconds is Constrained[I64, PgTimeValidator]

// Validates an I64 and returns (PgTimeMicroseconds | ValidationFailure).
type MakePgTimeMicroseconds is MakeConstrained[I64, PgTimeValidator]

class val PgTime is Equatable[PgTime]
  """
  A PostgreSQL `time` value. Stores microseconds since midnight.
  Valid range: 0 to 86,400,000,000 (exclusive). Constructed from a
  `PgTimeMicroseconds` value obtained via `MakePgTimeMicroseconds`.
  """
  let microseconds: I64

  new val create(microseconds': PgTimeMicroseconds) =>
    microseconds = microseconds'()

  fun eq(that: box->PgTime): Bool =>
    microseconds == that.microseconds

  fun ne(that: box->PgTime): Bool =>
    not eq(that)

  fun string(): String iso^ =>
    let us_per_second: I64 = 1_000_000
    let us_per_minute: I64 = us_per_second * 60
    let us_per_hour: I64 = us_per_minute * 60

    var remaining = microseconds
    let hour = remaining / us_per_hour
    remaining = remaining % us_per_hour
    let minute = remaining / us_per_minute
    remaining = remaining % us_per_minute
    let second = remaining / us_per_second
    let frac = remaining % us_per_second

    recover iso
      let s = String
      _TemporalFormat.append_two_digits(s, hour)
      s.append(":")
      _TemporalFormat.append_two_digits(s, minute)
      s.append(":")
      _TemporalFormat.append_two_digits(s, second)
      if frac != 0 then
        s.append(".")
        _TemporalFormat.append_fractional(s, frac)
      end
      s
    end

class val PgDate is Equatable[PgDate]
  """
  A PostgreSQL `date` value. Stores days since the PostgreSQL epoch
  (2000-01-01). Special values `I32.max_value()` and `I32.min_value()`
  represent positive and negative infinity respectively.
  """
  let days: I32

  new val create(days': I32) =>
    days = days'

  fun eq(that: box->PgDate): Bool =>
    days == that.days

  fun ne(that: box->PgDate): Bool =>
    not eq(that)

  fun string(): String iso^ =>
    if days == I32.max_value() then
      "infinity".clone()
    elseif days == I32.min_value() then
      "-infinity".clone()
    else
      (let year, let month, let day) =
        _TemporalFormat.days_to_date(days)
      recover iso
        let s = String
        _TemporalFormat.append_date(s, year, month, day)
        s
      end
    end

class val PgInterval is Equatable[PgInterval]
  """
  A PostgreSQL `interval` value. Stores three components: microseconds
  (time part), days, and months. All components can be negative.
  """
  let microseconds: I64
  let days: I32
  let months: I32

  new val create(microseconds': I64, days': I32, months': I32) =>
    microseconds = microseconds'
    days = days'
    months = months'

  fun eq(that: box->PgInterval): Bool =>
    (microseconds == that.microseconds) and (days == that.days) and
      (months == that.months)

  fun ne(that: box->PgInterval): Bool =>
    not eq(that)

  fun string(): String iso^ =>
    recover iso
      let s = String
      var has_output = false

      // Break months into years and months
      let total_months = months
      let years = total_months / 12
      let mons = total_months % 12

      if years != 0 then
        s.append(years.string())
        if (years == 1) or (years == -1) then
          s.append(" year")
        else
          s.append(" years")
        end
        has_output = true
      end

      if mons != 0 then
        if has_output then s.append(" ") end
        s.append(mons.string())
        if (mons == 1) or (mons == -1) then
          s.append(" mon")
        else
          s.append(" mons")
        end
        has_output = true
      end

      if days != 0 then
        if has_output then s.append(" ") end
        s.append(days.string())
        if (days == 1) or (days == -1) then
          s.append(" day")
        else
          s.append(" days")
        end
        has_output = true
      end

      // Time part
      let us_per_second: I64 = 1_000_000
      let us_per_minute: I64 = us_per_second * 60
      let us_per_hour: I64 = us_per_minute * 60

      var time_us = microseconds
      let negative_time = time_us < 0
      if negative_time then
        // Guard: -I64.min_value() wraps to itself in two's complement.
        // Clamp to I64.max_value() (loses 1us on a ~292M year interval).
        time_us = if time_us == I64.min_value() then
          I64.max_value()
        else
          -time_us
        end
      end

      let hour = time_us / us_per_hour
      time_us = time_us % us_per_hour
      let minute = time_us / us_per_minute
      time_us = time_us % us_per_minute
      let second = time_us / us_per_second
      let frac = time_us % us_per_second

      if (hour != 0) or (minute != 0) or (second != 0) or (frac != 0)
        or (not has_output)
      then
        if has_output then s.append(" ") end
        if negative_time then s.append("-") end
        _TemporalFormat.append_two_digits(s, hour)
        s.append(":")
        _TemporalFormat.append_two_digits(s, minute)
        s.append(":")
        _TemporalFormat.append_two_digits(s, second)
        if frac != 0 then
          s.append(".")
          _TemporalFormat.append_fractional(s, frac)
        end
      end

      s
    end

primitive _TemporalFormat
  fun pg_epoch_jdn(): I64 =>
    """
    Julian Day Number for the PostgreSQL epoch (2000-01-01).
    """
    2451545

  fun days_to_date(days: I32): (I32, I32, I32) =>
    """
    Convert days since 2000-01-01 to (year, month, day).
    """
    let j = days.i64() + pg_epoch_jdn()
    _julian_to_gregorian(j)

  fun _julian_to_gregorian(j: I64): (I32, I32, I32) =>
    """
    Convert a Julian Day Number to Gregorian (year, month, day).
    Uses the algorithm from the PostgreSQL source (j2date).
    """
    var julian = j
    julian = julian + 32044
    let g = julian / 146097
    let dg = julian % 146097
    let c = (((dg / 36524) + 1) * 3) / 4
    let dc = dg - (c * 36524)
    let b = dc / 1461
    let db = dc % 1461
    let a = (((db / 365) + 1) * 3) / 4
    let da = db - (a * 365)
    let y = ((g * 400) + (c * 100) + (b * 4)) + a
    let m = (((da * 5) + 308) / 153) - 2
    let d = (da - (((m + 4) * 153) / 5)) + 122
    let year = (y - 4800) + ((m + 2) / 12)
    let month = ((m + 2) % 12) + 1
    let day = d + 1
    (year.i32(), month.i32(), day.i32())

  fun append_date(s: String ref, year: I32, month: I32, day: I32) =>
    if year < 0 then
      s.append("-")
      _append_year(s, -year)
    else
      _append_year(s, year)
    end
    s.append("-")
    append_two_digits(s, month.i64())
    s.append("-")
    append_two_digits(s, day.i64())

  fun _append_year(s: String ref, year: I32) =>
    let y = year.u32()
    if y < 10 then
      s.append("000")
    elseif y < 100 then
      s.append("00")
    elseif y < 1000 then
      s.append("0")
    end
    s.append(y.string())

  fun append_two_digits(s: String ref, v: I64) =>
    let u = v.u64()
    if u < 10 then s.append("0") end
    s.append(u.string())

  fun append_fractional(s: String ref, frac: I64) =>
    """
    Append fractional seconds, trimming trailing zeros.
    Precondition: frac must be non-zero. The trailing-zero trimming loop
    does not terminate for frac=0. All callers guard with `if frac != 0`.
    """
    var f = frac.u64()
    var digits: USize = 6
    // Trim trailing zeros
    while (f % 10) == 0 do
      f = f / 10
      digits = digits - 1
    end
    // Pad leading zeros
    var temp = f
    var actual_digits: USize = 0
    while temp > 0 do
      actual_digits = actual_digits + 1
      temp = temp / 10
    end
    while actual_digits < digits do
      s.append("0")
      actual_digits = actual_digits + 1
    end
    s.append(f.string())
