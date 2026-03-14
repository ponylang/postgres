use "constrained_types"

primitive _BoolBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `bool` (OID 16).
  Encodes as 1 byte: 0x01 for true, 0x00 for false.
  Decodes any nonzero byte as true for robustness.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for Bool parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: Bool =>
      recover val [if v then U8(1) else U8(0) end] end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 1 then error end
    data(0)? != 0

primitive _ByteaBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `bytea` (OID 17).
  Binary format is raw bytes — no hex encoding overhead.
  Zero-length payloads are valid (empty byte array).

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for Array[U8] val parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: Array[U8] val => v
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    data

primitive _Int2BinaryCodec is Codec
  """
  Binary codec for PostgreSQL `int2` (OID 21).
  2 bytes, big-endian signed.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for I16 parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I16 =>
      recover val
        let a = Array[U8].init(0, 2)
        ifdef bigendian then
          a.update_u16(0, v.u16())?
        else
          a.update_u16(0, v.u16().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 2 then error end
    ifdef bigendian then
      data.read_u16(0)?.i16()
    else
      data.read_u16(0)?.bswap().i16()
    end

primitive _Int4BinaryCodec is Codec
  """
  Binary codec for PostgreSQL `int4` (OID 23).
  4 bytes, big-endian signed.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for I32 parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I32 =>
      recover val
        let a = Array[U8].init(0, 4)
        ifdef bigendian then
          a.update_u32(0, v.u32())?
        else
          a.update_u32(0, v.u32().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 4 then error end
    ifdef bigendian then
      data.read_u32(0)?.i32()
    else
      data.read_u32(0)?.bswap().i32()
    end

primitive _Int8BinaryCodec is Codec
  """
  Binary codec for PostgreSQL `int8` (OID 20).
  8 bytes, big-endian signed.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for I64 parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: I64 =>
      recover val
        let a = Array[U8].init(0, 8)
        ifdef bigendian then
          a.update_u64(0, v.u64())?
        else
          a.update_u64(0, v.u64().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 8 then error end
    ifdef bigendian then
      data.read_u64(0)?.i64()
    else
      data.read_u64(0)?.bswap().i64()
    end

primitive _Float4BinaryCodec is Codec
  """
  Binary codec for PostgreSQL `float4` (OID 700).
  4 bytes, IEEE 754 big-endian.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for F32 parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: F32 =>
      recover val
        let a = Array[U8].init(0, 4)
        ifdef bigendian then
          a.update_u32(0, v.bits())?
        else
          a.update_u32(0, v.bits().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 4 then error end
    ifdef bigendian then
      F32.from_bits(data.read_u32(0)?)
    else
      F32.from_bits(data.read_u32(0)?.bswap())
    end

primitive _Float8BinaryCodec is Codec
  """
  Binary codec for PostgreSQL `float8` (OID 701).
  8 bytes, IEEE 754 big-endian.

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for F64 parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: F64 =>
      recover val
        let a = Array[U8].init(0, 8)
        ifdef bigendian then
          a.update_u64(0, v.bits())?
        else
          a.update_u64(0, v.bits().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 8 then error end
    ifdef bigendian then
      F64.from_bits(data.read_u64(0)?)
    else
      F64.from_bits(data.read_u64(0)?.bswap())
    end

primitive _OidBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `oid` (OID 26).
  4 bytes, big-endian unsigned. Decodes to a numeric String.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String =>
      let v = s.u32()?
      recover val
        let a = Array[U8].init(0, 4)
        ifdef bigendian then
          a.update_u32(0, v)?
        else
          a.update_u32(0, v.bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 4 then error end
    let v = ifdef bigendian then
      data.read_u32(0)?
    else
      data.read_u32(0)?.bswap()
    end
    v.string()

primitive _NumericBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `numeric` (OID 1700).
  Variable-length: ndigits(I16) + weight(I16) + sign(I16) + dscale(I16)
  + base-10000 digit words. Decodes to a String preserving trailing zeros
  per `dscale`. Special values: NaN (sign 0xC000), Infinity (sign 0xD000),
  -Infinity (sign 0xF000).
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    // Encoding numeric from String is complex; error for now.
    // Phase 3 can add full encode support.
    error

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() < 8 then error end
    let ndigits = ifdef bigendian then
      data.read_u16(0)?.i16()
    else
      data.read_u16(0)?.bswap().i16()
    end
    let weight = ifdef bigendian then
      data.read_u16(2)?.i16()
    else
      data.read_u16(2)?.bswap().i16()
    end
    let sign = ifdef bigendian then
      data.read_u16(4)?
    else
      data.read_u16(4)?.bswap()
    end
    let dscale = ifdef bigendian then
      data.read_u16(6)?
    else
      data.read_u16(6)?.bswap()
    end

    if ndigits < 0 then error end

    // Special values
    if sign == 0xC000 then return "NaN" end
    if sign == 0xD000 then return "Infinity" end
    if sign == 0xF000 then return "-Infinity" end

    // Only 0x0000 (positive) and 0x4000 (negative) are valid for non-special
    if (sign != 0x0000) and (sign != 0x4000) then error end

    if data.size() != (8 + (ndigits.usize() * 2)) then error end

    // Read base-10000 digits
    let digits: Array[U16] val = recover val
      let ds = Array[U16](ndigits.usize())
      var idx: USize = 8
      var di: I16 = 0
      while di < ndigits do
        let d = ifdef bigendian then
          data.read_u16(idx)?
        else
          data.read_u16(idx)?.bswap()
        end
        ds.push(d)
        idx = idx + 2
        di = di + 1
      end
      ds
    end

    recover iso
      let s = String

      // Negative sign
      if sign == 0x4000 then s.append("-") end

      // Integer part
      if (weight < 0) and (ndigits > 0) then
        s.append("0")
      else
        // Use I32 counter to avoid infinite loop when weight = I16.max_value()
        // (I16 increment wraps from 32767 to -32768).
        var w: I32 = 0
        while w <= weight.i32() do
          let digit_idx = w.usize()
          let d: U16 = try digits(digit_idx)? else 0 end
          if w == 0 then
            // First digit: no leading zeros
            s.append(d.string())
          else
            // Subsequent digits: pad to 4
            _append_padded_digit(s, d)
          end
          w = w + 1
        end
        // When ndigits=0 (canonical zero), the while loop above still runs
        // once (w=0, w<=weight=0) and appends "0" via the `else 0` fallback
        // on the empty digits array. No extra append needed.
      end

      // Fractional part
      if dscale > 0 then
        s.append(".")
        var frac_digits_remaining = dscale.usize()
        var frac_idx = (weight + 1).i32()
        while frac_digits_remaining > 0 do
          let digit_idx = frac_idx.usize()
          let d: U16 = try digits(digit_idx)? else 0 end
          // How many digits from this base-10000 word?
          let avail = if frac_digits_remaining >= 4 then USize(4)
            else frac_digits_remaining
          end
          // Convert to 4-char string
          let ds = recover val
            let tmp = String(4)
            _append_padded_digit(tmp, d)
            tmp
          end
          s.append(ds.substring(0, avail.isize()))
          frac_digits_remaining = frac_digits_remaining - avail
          frac_idx = frac_idx + 1
        end
      end

      s
    end

  fun tag _append_padded_digit(s: String ref, d: U16) =>
    if d < 10 then
      s.append("000")
    elseif d < 100 then
      s.append("00")
    elseif d < 1000 then
      s.append("0")
    end
    s.append(d.string())

primitive _UuidBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `uuid` (OID 2950).
  16 bytes raw. Decodes to lowercase dash-separated hex.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String =>
      // Parse UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      if s.size() != 36 then error end
      recover val
        let a = Array[U8](16)
        var si: USize = 0
        var count: USize = 0
        while si < s.size() do
          if (si == 8) or (si == 13) or (si == 18) or (si == 23) then
            if s(si)? != '-' then error end
            si = si + 1
          else
            let hi = _hex_digit(s(si)?)?
            let lo = _hex_digit(s(si + 1)?)?
            a.push((hi * 16) + lo)
            count = count + 1
            si = si + 2
          end
        end
        if count != 16 then error end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 16 then error end
    recover val
      let s = String(36)
      var i: USize = 0
      while i < 16 do
        if (i == 4) or (i == 6) or (i == 8) or (i == 10) then
          s.push('-')
        end
        s.push(_to_hex(data(i)? >> 4))
        s.push(_to_hex(data(i)? and 0x0F))
        i = i + 1
      end
      s
    end

  fun _hex_digit(c: U8): U8 ? =>
    if (c >= '0') and (c <= '9') then c - '0'
    elseif (c >= 'a') and (c <= 'f') then (c - 'a') + 10
    elseif (c >= 'A') and (c <= 'F') then (c - 'A') + 10
    else error
    end

  fun _to_hex(nibble: U8): U8 =>
    if nibble < 10 then nibble + '0'
    else (nibble - 10) + 'a'
    end

primitive _JsonbBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `jsonb` (OID 3802).
  Binary format is 1 version byte (0x01) followed by JSON UTF-8 text.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String =>
      recover val
        let a = Array[U8](1 + s.size())
        a.push(1) // version byte
        a.append(s.array())
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() < 1 then error end
    if data(0)? != 1 then error end
    String.from_array(recover val data.trim(1) end)

primitive _DateBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `date` (OID 1082).
  4 bytes, big-endian signed I32 (days since 2000-01-01).

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for PgDate parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgDate =>
      recover val
        let a = Array[U8].init(0, 4)
        ifdef bigendian then
          a.update_u32(0, v.days.u32())?
        else
          a.update_u32(0, v.days.u32().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 4 then error end
    let days = ifdef bigendian then
      data.read_u32(0)?.i32()
    else
      data.read_u32(0)?.bswap().i32()
    end
    PgDate(days)

primitive _TimeBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `time` (OID 1083).
  8 bytes, big-endian signed I64 (microseconds since midnight).

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for PgTime parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgTime =>
      recover val
        let a = Array[U8].init(0, 8)
        ifdef bigendian then
          a.update_u64(0, v.microseconds.u64())?
        else
          a.update_u64(0, v.microseconds.u64().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 8 then error end
    let us = ifdef bigendian then
      data.read_u64(0)?.i64()
    else
      data.read_u64(0)?.bswap().i64()
    end
    PgTime(MakePgTimeMicroseconds(us) as PgTimeMicroseconds)

primitive _TimestampBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `timestamp` (OID 1114) and `timestamptz`
  (OID 1184). 8 bytes, big-endian signed I64 (microseconds since
  2000-01-01 00:00:00).

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for PgTimestamp parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgTimestamp =>
      recover val
        let a = Array[U8].init(0, 8)
        ifdef bigendian then
          a.update_u64(0, v.microseconds.u64())?
        else
          a.update_u64(0, v.microseconds.u64().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 8 then error end
    let us = ifdef bigendian then
      data.read_u64(0)?.i64()
    else
      data.read_u64(0)?.bswap().i64()
    end
    PgTimestamp(us)

primitive _IntervalBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `interval` (OID 1186).
  16 bytes: I64 BE (microseconds) + I32 BE (days) + I32 BE (months).

  Coupling: `_FrontendMessage.bind()` contains equivalent inline encoding
  for PgInterval parameters. Changes here must be mirrored there.
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let v: PgInterval =>
      recover val
        let a = Array[U8].init(0, 16)
        ifdef bigendian then
          a.update_u64(0, v.microseconds.u64())?
          a.update_u32(8, v.days.u32())?
          a.update_u32(12, v.months.u32())?
        else
          a.update_u64(0, v.microseconds.u64().bswap())?
          a.update_u32(8, v.days.u32().bswap())?
          a.update_u32(12, v.months.u32().bswap())?
        end
        a
      end
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes ? =>
    if data.size() != 16 then error end
    let us = ifdef bigendian then
      data.read_u64(0)?.i64()
    else
      data.read_u64(0)?.bswap().i64()
    end
    let d = ifdef bigendian then
      data.read_u32(8)?.i32()
    else
      data.read_u32(8)?.bswap().i32()
    end
    let m = ifdef bigendian then
      data.read_u32(12)?.i32()
    else
      data.read_u32(12)?.bswap().i32()
    end
    PgInterval(us, d, m)
