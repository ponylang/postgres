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
