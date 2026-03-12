primitive _BoolBinaryCodec is Codec
  """
  Binary codec for PostgreSQL `bool` (OID 16).
  Encodes as 1 byte: 0x01 for true, 0x00 for false.
  Decodes any nonzero byte as true for robustness.
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
