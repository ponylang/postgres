use "constrained_types"
use "pony_test"

class \nodoc\ iso _TestBoolBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Bool/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _BoolBinaryCodec
    h.assert_eq[U16](1, codec.format())

    let encoded_true = codec.encode(true)?
    h.assert_eq[USize](1, encoded_true.size())
    h.assert_eq[U8](1, encoded_true(0)?)
    h.assert_is[FieldData](true, codec.decode(encoded_true)?)

    let encoded_false = codec.encode(false)?
    h.assert_eq[USize](1, encoded_false.size())
    h.assert_eq[U8](0, encoded_false(0)?)
    h.assert_is[FieldData](false, codec.decode(encoded_false)?)

class \nodoc\ iso _TestBoolBinaryCodecNonzeroTrue is UnitTest
  fun name(): String =>
    "Codec/Binary/Bool/NonzeroTrue"

  fun apply(h: TestHelper) ? =>
    // Any nonzero byte decodes as true
    let data: Array[U8] val = recover val [42] end
    h.assert_is[FieldData](true, _BoolBinaryCodec.decode(data)?)

class \nodoc\ iso _TestBoolBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Bool/BadLength"

  fun apply(h: TestHelper) =>
    let data: Array[U8] val = recover val [0; 0] end
    h.assert_error({()? => _BoolBinaryCodec.decode(data)? })

class \nodoc\ iso _TestBoolBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Bool/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _BoolBinaryCodec.encode("not a bool")? })

class \nodoc\ iso _TestInt2BinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Int2/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int2BinaryCodec
    h.assert_eq[U16](1, codec.format())

    let values: Array[I16] = [0; 1; -1; I16.max_value(); I16.min_value()]
    for v in values.values() do
      let encoded = codec.encode(v)?
      h.assert_eq[USize](2, encoded.size())
      match codec.decode(encoded)?
      | let decoded: I16 => h.assert_eq[I16](v, decoded)
      else h.fail("Expected I16 from decode")
      end
    end

class \nodoc\ iso _TestInt2BinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Int2/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0] end
    h.assert_error({()? => _Int2BinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val [0; 0; 0] end
    h.assert_error({()? => _Int2BinaryCodec.decode(long)? })

class \nodoc\ iso _TestInt2BinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Int2/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _Int2BinaryCodec.encode("not an I16")? })

class \nodoc\ iso _TestInt4BinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Int4/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int4BinaryCodec
    h.assert_eq[U16](1, codec.format())

    let values: Array[I32] = [0; 1; -1; 42; -1000; I32.max_value()
      I32.min_value()]
    for v in values.values() do
      let encoded = codec.encode(v)?
      h.assert_eq[USize](4, encoded.size())
      match codec.decode(encoded)?
      | let decoded: I32 => h.assert_eq[I32](v, decoded)
      else h.fail("Expected I32 from decode")
      end
    end

class \nodoc\ iso _TestInt4BinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Int4/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0] end
    h.assert_error({()? => _Int4BinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val [0; 0; 0; 0; 0] end
    h.assert_error({()? => _Int4BinaryCodec.decode(long)? })

class \nodoc\ iso _TestInt4BinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Int4/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _Int4BinaryCodec.encode("not an I32")? })

class \nodoc\ iso _TestInt8BinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Int8/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int8BinaryCodec
    h.assert_eq[U16](1, codec.format())

    let values: Array[I64] = [0; 1; -1; 9999999999; -9999999999
      I64.max_value(); I64.min_value()]
    for v in values.values() do
      let encoded = codec.encode(v)?
      h.assert_eq[USize](8, encoded.size())
      match codec.decode(encoded)?
      | let decoded: I64 => h.assert_eq[I64](v, decoded)
      else h.fail("Expected I64 from decode")
      end
    end

class \nodoc\ iso _TestInt8BinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Int8/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0; 0] end
    h.assert_error({()? => _Int8BinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 9) end
    h.assert_error({()? => _Int8BinaryCodec.decode(long)? })

class \nodoc\ iso _TestInt8BinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Int8/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _Int8BinaryCodec.encode("not an I64")? })

class \nodoc\ iso _TestFloat4BinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Float4/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Float4BinaryCodec
    h.assert_eq[U16](1, codec.format())

    let values: Array[F32] = [F32(0); F32(1.5); F32(-3.14); F32.max_value()
      F32.min_value()]
    for v in values.values() do
      let encoded = codec.encode(v)?
      h.assert_eq[USize](4, encoded.size())
      match codec.decode(encoded)?
      | let decoded: F32 => h.assert_eq[F32](v, decoded)
      else h.fail("Expected F32 from decode")
      end
    end

class \nodoc\ iso _TestFloat4BinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Float4/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0] end
    h.assert_error({()? => _Float4BinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val [0; 0; 0; 0; 0] end
    h.assert_error({()? => _Float4BinaryCodec.decode(long)? })

class \nodoc\ iso _TestFloat4BinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Float4/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _Float4BinaryCodec.encode("not an F32")? })

class \nodoc\ iso _TestFloat8BinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Float8/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Float8BinaryCodec
    h.assert_eq[U16](1, codec.format())

    let values: Array[F64] = [F64(0); F64(1.5); F64(-3.14); F64.max_value()
      F64.min_value()]
    for v in values.values() do
      let encoded = codec.encode(v)?
      h.assert_eq[USize](8, encoded.size())
      match codec.decode(encoded)?
      | let decoded: F64 => h.assert_eq[F64](v, decoded)
      else h.fail("Expected F64 from decode")
      end
    end

class \nodoc\ iso _TestFloat8BinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Float8/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0; 0] end
    h.assert_error({()? => _Float8BinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 9) end
    h.assert_error({()? => _Float8BinaryCodec.decode(long)? })

class \nodoc\ iso _TestFloat8BinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Float8/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _Float8BinaryCodec.encode("not an F64")? })

class \nodoc\ iso _TestByteaBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Bytea/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _ByteaBinaryCodec
    h.assert_eq[U16](1, codec.format())

    let data: Array[U8] val = recover val [1; 2; 3; 255; 0] end
    let encoded = codec.encode(data)?
    // Binary bytea is pass-through
    h.assert_array_eq[U8](data, encoded)
    match codec.decode(encoded)
    | let decoded: Bytea => h.assert_array_eq[U8](data, decoded.data)
    else h.fail("Expected Bytea from decode")
    end

class \nodoc\ iso _TestByteaBinaryCodecEmpty is UnitTest
  fun name(): String =>
    "Codec/Binary/Bytea/Empty"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val Array[U8] end
    let encoded = _ByteaBinaryCodec.encode(data)?
    h.assert_eq[USize](0, encoded.size())

class \nodoc\ iso _TestBoolTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Bool/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _BoolTextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded_true = codec.encode(true)?
    h.assert_array_eq[U8]("t".array(), encoded_true)
    h.assert_is[FieldData](true, codec.decode(encoded_true))

    let encoded_false = codec.encode(false)?
    h.assert_array_eq[U8]("f".array(), encoded_false)
    h.assert_is[FieldData](false, codec.decode(encoded_false))

class \nodoc\ iso _TestInt4TextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Int4/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int4TextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode(I32(42))?
    h.assert_array_eq[U8]("42".array(), encoded)
    match codec.decode(encoded)?
    | let decoded: I32 => h.assert_eq[I32](42, decoded)
    else h.fail("Expected I32 from decode")
    end

class \nodoc\ iso _TestByteaTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Bytea/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _ByteaTextCodec
    let data: Array[U8] val = recover val [0xDE; 0xAD; 0xBE; 0xEF] end
    let encoded = codec.encode(data)?
    // Should produce hex format: \xdeadbeef
    h.assert_array_eq[U8]("\\xdeadbeef".array(), encoded)
    match codec.decode(encoded)?
    | let decoded: Bytea => h.assert_array_eq[U8](data, decoded.data)
    else h.fail("Expected Bytea from decode")
    end

class \nodoc\ iso _TestByteaTextCodecBadHex is UnitTest
  fun name(): String =>
    "Codec/Text/Bytea/BadHex"

  fun apply(h: TestHelper) =>
    // Missing \x prefix
    let no_prefix: Array[U8] val = "DEADBEEF".array()
    h.assert_error({()? => _ByteaTextCodec.decode(no_prefix)? })
    // Odd hex length
    let odd: Array[U8] val = "\\xDEA".array()
    h.assert_error({()? => _ByteaTextCodec.decode(odd)? })

class \nodoc\ iso _TestCodecRegistryDecodeUnknownText is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/UnknownText"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // Unknown OID in text format should fall back to String
    let data: Array[U8] val = "hello".array()
    match reg.decode(99999, 0, data)?
    | let s: String => h.assert_eq[String]("hello", s)
    else h.fail("Expected String fallback for unknown text OID")
    end

class \nodoc\ iso _TestCodecRegistryDecodeUnknownBinary is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/UnknownBinary"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // Unknown OID in binary format should fall back to raw bytes
    let data: Array[U8] val = recover val [1; 2; 3] end
    match reg.decode(99999, 1, data)?
    | let a: RawBytes => h.assert_array_eq[U8](data, a.data)
    else h.fail("Expected RawBytes fallback for unknown binary OID")
    end

class \nodoc\ iso _TestCodecRegistryDecodeKnown is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/Known"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 16 (bool) text format, "t" -> true
    let data: Array[U8] val = "t".array()
    h.assert_is[FieldData](true, reg.decode(16, 0, data)?)

class \nodoc\ iso _TestCodecRegistryHasBinaryCodec is UnitTest
  fun name(): String =>
    "CodecRegistry/HasBinaryCodec"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    h.assert_true(reg.has_binary_codec(16))   // bool
    h.assert_true(reg.has_binary_codec(23))   // int4
    h.assert_true(reg.has_binary_codec(701))  // float8
    h.assert_true(reg.has_binary_codec(25))   // text — text passthrough binary
    h.assert_true(reg.has_binary_codec(1082)) // date
    h.assert_true(reg.has_binary_codec(1114)) // timestamp
    h.assert_true(reg.has_binary_codec(1700)) // numeric
    h.assert_true(reg.has_binary_codec(2950)) // uuid
    h.assert_false(reg.has_binary_codec(99999))

class \nodoc\ iso _TestParamEncoderOids is UnitTest
  fun name(): String =>
    "ParamEncoder/Oids"

  fun apply(h: TestHelper) ? =>
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: I16(1); I32(2); I64(3); F32(1.0); F64(2.0)
        true; recover val [as U8: 0] end; "text"; None
        PgTimestamp(0)
        PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds)
        PgDate(0); PgInterval(0, 0, 0)]
    end
    let oids = _ParamEncoder.oids_for(params, CodecRegistry)
    h.assert_eq[USize](13, oids.size())
    try
      h.assert_eq[U32](21, oids(0)?)    // I16
      h.assert_eq[U32](23, oids(1)?)    // I32
      h.assert_eq[U32](20, oids(2)?)    // I64
      h.assert_eq[U32](700, oids(3)?)   // F32
      h.assert_eq[U32](701, oids(4)?)   // F64
      h.assert_eq[U32](16, oids(5)?)    // Bool
      h.assert_eq[U32](17, oids(6)?)    // Array[U8]
      h.assert_eq[U32](0, oids(7)?)     // String
      h.assert_eq[U32](0, oids(8)?)     // None
      h.assert_eq[U32](1114, oids(9)?)  // PgTimestamp
      h.assert_eq[U32](1083, oids(10)?) // PgTime
      h.assert_eq[U32](1082, oids(11)?) // PgDate
      h.assert_eq[U32](1186, oids(12)?) // PgInterval
    else
      h.fail("Unexpected error accessing OIDs array")
    end

class \nodoc\ iso _TestFrontendMessageBindWithBinaryI32 is UnitTest
  fun name(): String =>
    "FrontendMessage/BindWithBinaryI32"

  fun apply(h: TestHelper) ? =>
    // Bind("", "", [I32(42)])
    // Per-param format codes: 1 format code (binary=1 for I32)
    // params_data = 4 (length) + 4 (I32 bytes) = 8
    // Result format: num_result_formats=1, format_code=1 (binary)
    // Length = 4 + 0+1 + 0+1 + 2 + 1*2 + 2 + 8 + 2 + 2 = 24, total = 25
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: I32(42)]
    end
    let result = _FrontendMessage.bind("", "", params, CodecRegistry)?
    h.assert_eq[USize](25, result.size())

    // Check format code is binary (1)
    ifdef bigendian then
      // format code count
      h.assert_eq[U8](0, result(7)?)
      h.assert_eq[U8](1, result(8)?)
      // format code value = 1 (binary)
      h.assert_eq[U8](0, result(9)?)
      h.assert_eq[U8](1, result(10)?)
    else
      // format code count = 1
      h.assert_eq[U8](0, result(7)?)
      h.assert_eq[U8](1, result(8)?)
      // format code value = 1 (binary)
      h.assert_eq[U8](0, result(9)?)
      h.assert_eq[U8](1, result(10)?)
    end

class \nodoc\ iso _TestFrontendMessageBindMixedParams is UnitTest
  fun name(): String =>
    "FrontendMessage/BindMixedParams"

  fun apply(h: TestHelper) ? =>
    // Bind("", "", ["hello"; I32(42); None])
    // Format codes: text(0), binary(1), text(0)
    // params_data = (4+5) + (4+4) + (4) = 21
    // Result format: num_result_formats=1, format_code=1 (binary)
    // Length = 4 + 0+1 + 0+1 + 2 + 3*2 + 2 + 21 + 2 + 2 = 41, total = 42
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: "hello"; I32(42); None]
    end
    let result = _FrontendMessage.bind("", "", params, CodecRegistry)?
    h.assert_eq[USize](42, result.size())

class \nodoc\ iso _TestFrontendMessageBindEmptyParams is UnitTest
  fun name(): String =>
    "FrontendMessage/BindEmptyParams"

  fun apply(h: TestHelper) ? =>
    // Bind("", "", [])
    // No format codes, no params
    // Result format: num_result_formats=1, format_code=1 (binary)
    // Length = 4 + 0+1 + 0+1 + 2 + 0 + 2 + 0 + 2 + 2 = 14, total = 15
    let params: Array[FieldDataTypes] val = recover val Array[FieldDataTypes] end
    let result = _FrontendMessage.bind("", "", params, CodecRegistry)?
    h.assert_eq[USize](15, result.size())

class \nodoc\ iso _TestFrontendMessageBindTemporalParams is UnitTest
  fun name(): String =>
    "FrontendMessage/BindTemporalParams"

  fun apply(h: TestHelper) ? =>
    // Bind with one of each temporal type to verify size and format codes.
    // PgTimestamp: 8 bytes, PgTime: 8 bytes, PgDate: 4 bytes,
    // PgInterval: 16 bytes = 36 data bytes + 4*4 length fields = 52
    // Length = 4 + 0+1 + 0+1 + 2 + 4*2 + 2 + 52 + 2 + 2 = 74, total = 75
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes:
        PgTimestamp(1_000_000) // 1 second after epoch
        PgTime(MakePgTimeMicroseconds(3_600_000_000) as PgTimeMicroseconds)
        PgDate(365)            // 2001-01-01
        PgInterval(0, 30, 1)] // 1 mon 30 days
    end
    let result = _FrontendMessage.bind("", "", params, CodecRegistry)?
    h.assert_eq[USize](75, result.size())

    // All 4 format codes should be binary (1)
    // Format codes start at offset 7 (after 'B' + 4-byte length + 2 nulls)
    // num_param_formats at offset 7-8, then 4 format code pairs
    try
      var off: USize = 9  // first format code
      var i: USize = 0
      while i < 4 do
        h.assert_eq[U8](0, result(off)?)
        h.assert_eq[U8](1, result(off + 1)?)
        off = off + 2
        i = i + 1
      end
    else
      h.fail("Error reading format codes")
    end

    // Verify the encoded PgTimestamp value (first param data).
    // After format codes (8 bytes) + num_params (2 bytes) = offset 19
    // val_len (4 bytes) = 8, then 8 bytes of big-endian I64
    let ts_val_offset: USize = 19
    // val_len = 8
    h.assert_eq[U8](0, result(ts_val_offset)?)
    h.assert_eq[U8](0, result(ts_val_offset + 1)?)
    h.assert_eq[U8](0, result(ts_val_offset + 2)?)
    h.assert_eq[U8](8, result(ts_val_offset + 3)?)
    // 1_000_000 = 0x00000000000F4240 big-endian
    h.assert_eq[U8](0x00, result(ts_val_offset + 4)?)
    h.assert_eq[U8](0x00, result(ts_val_offset + 5)?)
    h.assert_eq[U8](0x00, result(ts_val_offset + 6)?)
    h.assert_eq[U8](0x00, result(ts_val_offset + 7)?)
    h.assert_eq[U8](0x00, result(ts_val_offset + 8)?)
    h.assert_eq[U8](0x0F, result(ts_val_offset + 9)?)
    h.assert_eq[U8](0x42, result(ts_val_offset + 10)?)
    h.assert_eq[U8](0x40, result(ts_val_offset + 11)?)

    // Verify PgDate value (third param).
    // After PgTimestamp (4+8=12) + PgTime (4+8=12) = offset 19+24 = 43
    let date_val_offset: USize = 43
    // val_len = 4
    h.assert_eq[U8](0, result(date_val_offset)?)
    h.assert_eq[U8](0, result(date_val_offset + 1)?)
    h.assert_eq[U8](0, result(date_val_offset + 2)?)
    h.assert_eq[U8](4, result(date_val_offset + 3)?)
    // 365 = 0x0000016D big-endian
    h.assert_eq[U8](0x00, result(date_val_offset + 4)?)
    h.assert_eq[U8](0x00, result(date_val_offset + 5)?)
    h.assert_eq[U8](0x01, result(date_val_offset + 6)?)
    h.assert_eq[U8](0x6D, result(date_val_offset + 7)?)

// ---------------------------------------------------------------------------
// TextPassthroughBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTextPassthroughBinaryCodecDecode is UnitTest
  fun name(): String =>
    "Codec/Binary/TextPassthrough/Decode"

  fun apply(h: TestHelper) =>
    let codec = _TextPassthroughBinaryCodec
    h.assert_eq[U16](1, codec.format())

    let data: Array[U8] val = "hello world".array()
    match codec.decode(data)
    | let s: String => h.assert_eq[String]("hello world", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestTextPassthroughBinaryCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Binary/TextPassthrough/Encode"

  fun apply(h: TestHelper) ? =>
    let encoded = _TextPassthroughBinaryCodec.encode("hello")?
    h.assert_array_eq[U8]("hello".array(), encoded)

class \nodoc\ iso _TestTextPassthroughBinaryCodecEmpty is UnitTest
  fun name(): String =>
    "Codec/Binary/TextPassthrough/Empty"

  fun apply(h: TestHelper) =>
    let data: Array[U8] val = recover val Array[U8] end
    match _TextPassthroughBinaryCodec.decode(data)
    | let s: String => h.assert_eq[String]("", s)
    else h.fail("Expected empty String from decode")
    end

class \nodoc\ iso _TestTextPassthroughBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/TextPassthrough/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _TextPassthroughBinaryCodec.encode(I32(42))? })

// ---------------------------------------------------------------------------
// OidBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestOidBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Oid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _OidBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // Encode "12345" -> 4 bytes -> decode back to "12345"
    let encoded = codec.encode("12345")?
    h.assert_eq[USize](4, encoded.size())
    match codec.decode(encoded)?
    | let s: String => h.assert_eq[String]("12345", s)
    else h.fail("Expected String from decode")
    end

    // Zero
    let encoded_zero = codec.encode("0")?
    match codec.decode(encoded_zero)?
    | let s: String => h.assert_eq[String]("0", s)
    else h.fail("Expected '0' from decode")
    end

    // Max U32
    let encoded_max = codec.encode(U32.max_value().string())?
    match codec.decode(encoded_max)?
    | let s: String => h.assert_eq[String](U32.max_value().string(), s)
    else h.fail("Expected max U32 string from decode")
    end

class \nodoc\ iso _TestOidBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Oid/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0] end
    h.assert_error({()? => _OidBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val [0; 0; 0; 0; 0] end
    h.assert_error({()? => _OidBinaryCodec.decode(long)? })

class \nodoc\ iso _TestOidBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Oid/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _OidBinaryCodec.encode(I32(42))? })

// ---------------------------------------------------------------------------
// NumericBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestNumericBinaryCodecPositiveInteger is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/PositiveInteger"

  fun apply(h: TestHelper) ? =>
    let codec = _NumericBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // 12345: ndigits=2, weight=1, sign=0x0000(+), dscale=0
    // digits: 1 (weight 1 = 1*10000), 2345 (weight 0)
    let data: Array[U8] val = recover val
      let a = Array[U8]
      // ndigits = 2
      a.push(0); a.push(2)
      // weight = 1
      a.push(0); a.push(1)
      // sign = 0x0000 (positive)
      a.push(0); a.push(0)
      // dscale = 0
      a.push(0); a.push(0)
      // digit[0] = 1
      a.push(0); a.push(1)
      // digit[1] = 2345
      a.push(0x09); a.push(0x29)
      a
    end
    match codec.decode(data)?
    | let s: String => h.assert_eq[String]("12345", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecNegative is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/Negative"

  fun apply(h: TestHelper) ? =>
    // -42: ndigits=1, weight=0, sign=0x4000(-), dscale=0
    // digit[0] = 42
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(1)    // ndigits = 1
      a.push(0); a.push(0)    // weight = 0
      a.push(0x40); a.push(0) // sign = 0x4000
      a.push(0); a.push(0)    // dscale = 0
      a.push(0); a.push(42)   // digit = 42
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("-42", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecNaN is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/NaN"

  fun apply(h: TestHelper) ? =>
    // NaN: ndigits=0, weight=0, sign=0xC000, dscale=0
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(0)    // ndigits = 0
      a.push(0); a.push(0)    // weight = 0
      a.push(0xC0); a.push(0) // sign = 0xC000
      a.push(0); a.push(0)    // dscale = 0
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("NaN", s)
    else h.fail("Expected 'NaN' from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecZero is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/Zero"

  fun apply(h: TestHelper) ? =>
    // Canonical zero: ndigits=0, weight=0, sign=0x0000, dscale=0
    // This is how PostgreSQL actually sends 0::numeric on the wire.
    let canonical: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(0) // ndigits = 0
      a.push(0); a.push(0) // weight = 0
      a.push(0); a.push(0) // sign = 0x0000
      a.push(0); a.push(0) // dscale = 0
      a
    end
    match _NumericBinaryCodec.decode(canonical)?
    | let s: String => h.assert_eq[String]("0", s)
    else h.fail("Expected '0' from canonical zero decode")
    end

    // Zero with dscale: ndigits=0, weight=0, sign=0x0000, dscale=2
    // This is how PostgreSQL sends 0.00::numeric.
    let zero_dscale: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(0) // ndigits = 0
      a.push(0); a.push(0) // weight = 0
      a.push(0); a.push(0) // sign = 0x0000
      a.push(0); a.push(2) // dscale = 2
      a
    end
    match _NumericBinaryCodec.decode(zero_dscale)?
    | let s: String => h.assert_eq[String]("0.00", s)
    else h.fail("Expected '0.00' from zero dscale decode")
    end

    // Non-canonical zero: ndigits=1, weight=0, digit[0]=0
    // Some implementations may send this form.
    let noncanon: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(1) // ndigits = 1
      a.push(0); a.push(0) // weight = 0
      a.push(0); a.push(0) // sign = 0x0000
      a.push(0); a.push(0) // dscale = 0
      a.push(0); a.push(0) // digit[0] = 0
      a
    end
    match _NumericBinaryCodec.decode(noncanon)?
    | let s: String => h.assert_eq[String]("0", s)
    else h.fail("Expected '0' from non-canonical zero decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecFractional is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/Fractional"

  fun apply(h: TestHelper) ? =>
    // 3.14: ndigits=2, weight=0, sign=0x0000, dscale=2
    // digit[0] = 3 (weight 0), digit[1] = 1400 (fractional, only first 2 used)
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(2)    // ndigits = 2
      a.push(0); a.push(0)    // weight = 0
      a.push(0); a.push(0)    // sign = 0x0000
      a.push(0); a.push(2)    // dscale = 2
      a.push(0); a.push(3)    // digit[0] = 3
      a.push(0x05); a.push(0x78) // digit[1] = 1400
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("3.14", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecPreservedDscale is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/PreservedDscale"

  fun apply(h: TestHelper) ? =>
    // 1.00: ndigits=1, weight=0, sign=0x0000, dscale=2
    // Only integer digit, dscale forces ".00"
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(1)  // ndigits = 1
      a.push(0); a.push(0)  // weight = 0
      a.push(0); a.push(0)  // sign = 0x0000
      a.push(0); a.push(2)  // dscale = 2
      a.push(0); a.push(1)  // digit[0] = 1
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("1.00", s)
    else h.fail("Expected '1.00' from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecLessThanOne is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/LessThanOne"

  fun apply(h: TestHelper) ? =>
    // 0.5: ndigits=1, weight=-1, sign=0x0000, dscale=1
    // digit[0] = 5000 (first fractional group, only 1 digit used by dscale)
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(1)            // ndigits = 1
      a.push(0xFF); a.push(0xFF)      // weight = -1
      a.push(0); a.push(0)            // sign = 0x0000
      a.push(0); a.push(1)            // dscale = 1
      a.push(0x13); a.push(0x88)      // digit[0] = 5000
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("0.5", s)
    else h.fail("Expected '0.5' from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecTooShort is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/TooShort"

  fun apply(h: TestHelper) =>
    // Less than 8 bytes header
    let short: Array[U8] val = recover val [0; 0; 0; 0; 0; 0; 0] end
    h.assert_error({()? => _NumericBinaryCodec.decode(short)? })

class \nodoc\ iso _TestNumericBinaryCodecNdigitsMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/NdigitsMismatch"

  fun apply(h: TestHelper) =>
    // Header says ndigits=2 but only 1 digit word provided (10 bytes
    // instead of 12). Should error on the size validation.
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(2) // ndigits = 2
      a.push(0); a.push(0) // weight = 0
      a.push(0); a.push(0) // sign = 0x0000
      a.push(0); a.push(0) // dscale = 0
      a.push(0); a.push(42) // only 1 digit word
      a
    end
    h.assert_error({()? => _NumericBinaryCodec.decode(data)? })

class \nodoc\ iso _TestNumericBinaryCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/Infinity"

  fun apply(h: TestHelper) ? =>
    // Positive infinity: sign = 0xD000
    let pos_inf: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(0) // ndigits = 0
      a.push(0); a.push(0) // weight = 0
      a.push(0xD0); a.push(0) // sign = 0xD000
      a.push(0); a.push(0) // dscale = 0
      a
    end
    match _NumericBinaryCodec.decode(pos_inf)?
    | let s: String => h.assert_eq[String]("Infinity", s)
    else h.fail("Expected 'Infinity' from decode")
    end

    // Negative infinity: sign = 0xF000
    let neg_inf: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(0) // ndigits = 0
      a.push(0); a.push(0) // weight = 0
      a.push(0xF0); a.push(0) // sign = 0xF000
      a.push(0); a.push(0) // dscale = 0
      a
    end
    match _NumericBinaryCodec.decode(neg_inf)?
    | let s: String => h.assert_eq[String]("-Infinity", s)
    else h.fail("Expected '-Infinity' from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecEncodeErrors is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/EncodeErrors"

  fun apply(h: TestHelper) =>
    // Non-String types should error
    h.assert_error({()? => _NumericBinaryCodec.encode(I32(42))? })
    h.assert_error({()? => _NumericBinaryCodec.encode(true)? })
    // Invalid string formats should error
    h.assert_error({()? => _NumericBinaryCodec.encode("abc")? })

class \nodoc\ iso _TestNumericBinaryCodecBadSign is UnitTest
  """
  Numeric payloads with unrecognized sign values are rejected.
  """
  fun name(): String =>
    "Codec/Binary/Numeric/BadSign"

  fun apply(h: TestHelper) =>
    // Big-endian wire format: ndigits=0, weight=0, sign=0x8000, dscale=0
    let data: Array[U8] val = recover val
      [0x00; 0x00  // ndigits = 0
       0x00; 0x00  // weight = 0
       0x80; 0x00  // sign = 0x8000 (invalid)
       0x00; 0x00] // dscale = 0
    end
    h.assert_error({()? => _NumericBinaryCodec.decode(data)? })

class \nodoc\ iso _TestNumericBinaryCodecMaxWeight is UnitTest
  """
  Numeric with weight = I16.max_value() must not infinite-loop.
  The payload has ndigits=0, so the loop counter iterates from 0 to 32767
  emitting "0000" groups. This verifies the loop terminates.
  """
  fun name(): String =>
    "Codec/Binary/Numeric/MaxWeight"

  fun apply(h: TestHelper) ? =>
    // Big-endian wire format: ndigits=0, weight=32767, sign=0x0000, dscale=0
    let data: Array[U8] val = recover val
      [0x00; 0x00  // ndigits = 0
       0x7F; 0xFF  // weight = 32767 (I16.max_value())
       0x00; 0x00  // sign = positive
       0x00; 0x00] // dscale = 0
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String =>
      // "0" (first digit, no padding) + 32767 * "0000" = 1 + 131068 = 131069
      h.assert_eq[USize](131069, s.size())
    else h.fail("Expected String from numeric decode")
    end

// ---------------------------------------------------------------------------
// UuidBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestUuidBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Uuid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _UuidBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // 550e8400-e29b-41d4-a716-446655440000
    let data: Array[U8] val = recover val [
      0x55; 0x0e; 0x84; 0x00; 0xe2; 0x9b; 0x41; 0xd4
      0xa7; 0x16; 0x44; 0x66; 0x55; 0x44; 0x00; 0x00
    ] end
    match codec.decode(data)?
    | let s: String =>
      h.assert_eq[String]("550e8400-e29b-41d4-a716-446655440000", s)
    else h.fail("Expected String from decode")
    end

    // Roundtrip: encode the decoded string and compare bytes
    let reencoded = codec.encode("550e8400-e29b-41d4-a716-446655440000")?
    h.assert_array_eq[U8](data, reencoded)

class \nodoc\ iso _TestUuidBinaryCodecAllZeros is UnitTest
  fun name(): String =>
    "Codec/Binary/Uuid/AllZeros"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val Array[U8].init(0, 16) end
    match _UuidBinaryCodec.decode(data)?
    | let s: String =>
      h.assert_eq[String]("00000000-0000-0000-0000-000000000000", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestUuidBinaryCodecAllFF is UnitTest
  fun name(): String =>
    "Codec/Binary/Uuid/AllFF"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val Array[U8].init(0xFF, 16) end
    match _UuidBinaryCodec.decode(data)?
    | let s: String =>
      h.assert_eq[String]("ffffffff-ffff-ffff-ffff-ffffffffffff", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestUuidBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Uuid/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val Array[U8].init(0, 15) end
    h.assert_error({()? => _UuidBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 17) end
    h.assert_error({()? => _UuidBinaryCodec.decode(long)? })

class \nodoc\ iso _TestUuidBinaryCodecBadStringFormat is UnitTest
  fun name(): String =>
    "Codec/Binary/Uuid/BadStringFormat"

  fun apply(h: TestHelper) =>
    // Wrong length
    h.assert_error({()? => _UuidBinaryCodec.encode("too-short")? })
    // Missing dashes
    h.assert_error(
      {()? => _UuidBinaryCodec.encode(
        "550e8400xe29b-41d4-a716-446655440000")? })

// ---------------------------------------------------------------------------
// JsonbBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestJsonbBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Jsonb/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _JsonbBinaryCodec
    h.assert_eq[U16](1, codec.format())

    let json = """{"key": "value"}"""

    // Encode: version byte + JSON
    let encoded = codec.encode(json)?
    h.assert_eq[USize](json.size() + 1, encoded.size())
    h.assert_eq[U8](1, encoded(0)?)

    // Decode back
    match codec.decode(encoded)?
    | let s: String => h.assert_eq[String](json, s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestJsonbBinaryCodecBadVersion is UnitTest
  fun name(): String =>
    "Codec/Binary/Jsonb/BadVersion"

  fun apply(h: TestHelper) =>
    // Version byte 0x02 is unsupported
    let data: Array[U8] val = recover val [2; '{'; '}'] end
    h.assert_error({()? => _JsonbBinaryCodec.decode(data)? })

class \nodoc\ iso _TestJsonbBinaryCodecEmpty is UnitTest
  fun name(): String =>
    "Codec/Binary/Jsonb/Empty"

  fun apply(h: TestHelper) =>
    // No data at all
    let data: Array[U8] val = recover val Array[U8] end
    h.assert_error({()? => _JsonbBinaryCodec.decode(data)? })

class \nodoc\ iso _TestJsonbBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Jsonb/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _JsonbBinaryCodec.encode(I32(42))? })

// ---------------------------------------------------------------------------
// DateBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestDateBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Date/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _DateBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // Epoch (2000-01-01) = 0 days
    let epoch = PgDate(0)
    let encoded_epoch = codec.encode(epoch)?
    h.assert_eq[USize](4, encoded_epoch.size())
    match codec.decode(encoded_epoch)?
    | let d: PgDate => h.assert_true(epoch == d)
    else h.fail("Expected PgDate from decode")
    end

    // Positive days
    let pos = PgDate(8765)
    let encoded_pos = codec.encode(pos)?
    match codec.decode(encoded_pos)?
    | let d: PgDate => h.assert_true(pos == d)
    else h.fail("Expected PgDate from decode")
    end

    // Negative days
    let neg = PgDate(-365)
    let encoded_neg = codec.encode(neg)?
    match codec.decode(encoded_neg)?
    | let d: PgDate => h.assert_true(neg == d)
    else h.fail("Expected PgDate from decode")
    end

class \nodoc\ iso _TestDateBinaryCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Binary/Date/Infinity"

  fun apply(h: TestHelper) ? =>
    let codec = _DateBinaryCodec

    // Positive infinity (I32.max_value)
    let pos_inf = PgDate(I32.max_value())
    let encoded_pos = codec.encode(pos_inf)?
    match codec.decode(encoded_pos)?
    | let d: PgDate => h.assert_true(pos_inf == d)
    else h.fail("Expected PgDate from decode")
    end

    // Negative infinity (I32.min_value)
    let neg_inf = PgDate(I32.min_value())
    let encoded_neg = codec.encode(neg_inf)?
    match codec.decode(encoded_neg)?
    | let d: PgDate => h.assert_true(neg_inf == d)
    else h.fail("Expected PgDate from decode")
    end

class \nodoc\ iso _TestDateBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Date/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0] end
    h.assert_error({()? => _DateBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val [0; 0; 0; 0; 0] end
    h.assert_error({()? => _DateBinaryCodec.decode(long)? })

class \nodoc\ iso _TestDateBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Date/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _DateBinaryCodec.encode("2024-01-15")? })

// ---------------------------------------------------------------------------
// TimeBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTimeBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Time/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _TimeBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // Midnight = 0 microseconds
    let midnight =
      PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds)
    let encoded_mid = codec.encode(midnight)?
    h.assert_eq[USize](8, encoded_mid.size())
    match codec.decode(encoded_mid)?
    | let t: PgTime => h.assert_true(midnight == t)
    else h.fail("Expected PgTime from decode")
    end

    // 14:30:00 = 52200000000 microseconds
    let afternoon =
      PgTime(MakePgTimeMicroseconds(52_200_000_000) as PgTimeMicroseconds)
    let encoded_aft = codec.encode(afternoon)?
    match codec.decode(encoded_aft)?
    | let t: PgTime => h.assert_true(afternoon == t)
    else h.fail("Expected PgTime from decode")
    end

    // With fractional seconds: 14:30:00.123456 = 52200123456 us
    let fractional =
      PgTime(MakePgTimeMicroseconds(52_200_123_456) as PgTimeMicroseconds)
    let encoded_frac = codec.encode(fractional)?
    match codec.decode(encoded_frac)?
    | let t: PgTime => h.assert_true(fractional == t)
    else h.fail("Expected PgTime from decode")
    end

class \nodoc\ iso _TestTimeBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Time/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0; 0] end
    h.assert_error({()? => _TimeBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 9) end
    h.assert_error({()? => _TimeBinaryCodec.decode(long)? })

class \nodoc\ iso _TestTimeBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Time/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _TimeBinaryCodec.encode("14:30:00")? })

// ---------------------------------------------------------------------------
// TimestampBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTimestampBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Timestamp/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _TimestampBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // Epoch (2000-01-01 00:00:00) = 0 microseconds
    let epoch = PgTimestamp(0)
    let encoded = codec.encode(epoch)?
    h.assert_eq[USize](8, encoded.size())
    match codec.decode(encoded)?
    | let t: PgTimestamp => h.assert_true(epoch == t)
    else h.fail("Expected PgTimestamp from decode")
    end

    // Positive (2025-01-15 14:30:00)
    let positive = PgTimestamp(788_918_400_000_000)
    let encoded_pos = codec.encode(positive)?
    match codec.decode(encoded_pos)?
    | let t: PgTimestamp => h.assert_true(positive == t)
    else h.fail("Expected PgTimestamp from decode")
    end

    // Negative (before 2000-01-01)
    let negative = PgTimestamp(-86_400_000_000)
    let encoded_neg = codec.encode(negative)?
    match codec.decode(encoded_neg)?
    | let t: PgTimestamp => h.assert_true(negative == t)
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestampBinaryCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Binary/Timestamp/Infinity"

  fun apply(h: TestHelper) ? =>
    let codec = _TimestampBinaryCodec

    // Positive infinity (I64.max_value)
    let pos_inf = PgTimestamp(I64.max_value())
    let encoded_pos = codec.encode(pos_inf)?
    match codec.decode(encoded_pos)?
    | let t: PgTimestamp => h.assert_true(pos_inf == t)
    else h.fail("Expected PgTimestamp from decode")
    end

    // Negative infinity (I64.min_value)
    let neg_inf = PgTimestamp(I64.min_value())
    let encoded_neg = codec.encode(neg_inf)?
    match codec.decode(encoded_neg)?
    | let t: PgTimestamp => h.assert_true(neg_inf == t)
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestampBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Timestamp/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val [0; 0; 0; 0] end
    h.assert_error({()? => _TimestampBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 9) end
    h.assert_error({()? => _TimestampBinaryCodec.decode(long)? })

class \nodoc\ iso _TestTimestampBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Timestamp/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error(
      {()? => _TimestampBinaryCodec.encode("2024-01-15 14:30:00")? })

// ---------------------------------------------------------------------------
// IntervalBinaryCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestIntervalBinaryCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Interval/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _IntervalBinaryCodec
    h.assert_eq[U16](1, codec.format())

    // 1 year 2 months 3 days 04:05:06 =
    //   months=14, days=3, microseconds=14706000000
    let interval = PgInterval(14_706_000_000, 3, 14)
    let encoded = codec.encode(interval)?
    h.assert_eq[USize](16, encoded.size())
    match codec.decode(encoded)?
    | let i: PgInterval => h.assert_true(interval == i)
    else h.fail("Expected PgInterval from decode")
    end

    // Zero interval
    let zero = PgInterval(0, 0, 0)
    let encoded_zero = codec.encode(zero)?
    match codec.decode(encoded_zero)?
    | let i: PgInterval => h.assert_true(zero == i)
    else h.fail("Expected PgInterval from decode")
    end

    // Negative values
    let neg = PgInterval(-3_600_000_000, -5, -3)
    let encoded_neg = codec.encode(neg)?
    match codec.decode(encoded_neg)?
    | let i: PgInterval => h.assert_true(neg == i)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalBinaryCodecBadLength is UnitTest
  fun name(): String =>
    "Codec/Binary/Interval/BadLength"

  fun apply(h: TestHelper) =>
    let short: Array[U8] val = recover val Array[U8].init(0, 15) end
    h.assert_error({()? => _IntervalBinaryCodec.decode(short)? })
    let long: Array[U8] val = recover val Array[U8].init(0, 17) end
    h.assert_error({()? => _IntervalBinaryCodec.decode(long)? })

class \nodoc\ iso _TestIntervalBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Interval/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _IntervalBinaryCodec.encode("1 day")? })

// ---------------------------------------------------------------------------
// DateTextCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestDateTextCodecDecode is UnitTest
  fun name(): String =>
    "Codec/Text/Date/Decode"

  fun apply(h: TestHelper) ? =>
    let codec = _DateTextCodec
    h.assert_eq[U16](0, codec.format())

    // 2024-01-15
    match codec.decode("2024-01-15".array())?
    | let d: PgDate =>
      // Verify via string roundtrip
      h.assert_eq[String]("2024-01-15", d.string())
    else h.fail("Expected PgDate from decode")
    end

    // 2000-01-01 (epoch) should be day 0
    match codec.decode("2000-01-01".array())?
    | let d: PgDate => h.assert_eq[I32](0, d.days)
    else h.fail("Expected PgDate from decode")
    end

class \nodoc\ iso _TestDateTextCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Text/Date/Infinity"

  fun apply(h: TestHelper) ? =>
    match _DateTextCodec.decode("infinity".array())?
    | let d: PgDate => h.assert_eq[I32](I32.max_value(), d.days)
    else h.fail("Expected PgDate from decode")
    end

    match _DateTextCodec.decode("-infinity".array())?
    | let d: PgDate => h.assert_eq[I32](I32.min_value(), d.days)
    else h.fail("Expected PgDate from decode")
    end

class \nodoc\ iso _TestDateTextCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Text/Date/Encode"

  fun apply(h: TestHelper) ? =>
    let encoded = _DateTextCodec.encode(PgDate(0))?
    h.assert_eq[String]("2000-01-01", String.from_array(encoded))

// ---------------------------------------------------------------------------
// TimeTextCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTimeTextCodecDecode is UnitTest
  fun name(): String =>
    "Codec/Text/Time/Decode"

  fun apply(h: TestHelper) ? =>
    let codec = _TimeTextCodec
    h.assert_eq[U16](0, codec.format())

    // 14:30:00
    match codec.decode("14:30:00".array())?
    | let t: PgTime =>
      h.assert_eq[I64](52_200_000_000, t.microseconds)
    else h.fail("Expected PgTime from decode")
    end

    // 00:00:00 (midnight)
    match codec.decode("00:00:00".array())?
    | let t: PgTime => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTime from decode")
    end

class \nodoc\ iso _TestTimeTextCodecFractional is UnitTest
  fun name(): String =>
    "Codec/Text/Time/Fractional"

  fun apply(h: TestHelper) ? =>
    // 14:30:00.123456
    match _TimeTextCodec.decode("14:30:00.123456".array())?
    | let t: PgTime =>
      h.assert_eq[I64](52_200_123_456, t.microseconds)
    else h.fail("Expected PgTime from decode")
    end

    // Shorter fractional: 14:30:00.5 = 500000 us fraction
    match _TimeTextCodec.decode("14:30:00.5".array())?
    | let t: PgTime =>
      h.assert_eq[I64](52_200_500_000, t.microseconds)
    else h.fail("Expected PgTime from decode")
    end

class \nodoc\ iso _TestTimeTextCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Text/Time/Encode"

  fun apply(h: TestHelper) ? =>
    let encoded = _TimeTextCodec.encode(
      PgTime(MakePgTimeMicroseconds(52_200_000_000) as PgTimeMicroseconds))?
    h.assert_eq[String]("14:30:00", String.from_array(encoded))

// ---------------------------------------------------------------------------
// TimestampTextCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTimestampTextCodecDecode is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/Decode"

  fun apply(h: TestHelper) ? =>
    let codec = _TimestampTextCodec
    h.assert_eq[U16](0, codec.format())

    // 2024-01-15 14:30:00
    match codec.decode("2024-01-15 14:30:00".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

    // 2000-01-01 00:00:00 should be 0 microseconds
    match codec.decode("2000-01-01 00:00:00".array())?
    | let t: PgTimestamp =>
      h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestampTextCodecFractional is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/Fractional"

  fun apply(h: TestHelper) ? =>
    // 2024-01-15 14:30:00.123456
    match _TimestampTextCodec.decode("2024-01-15 14:30:00.123456".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00.123456", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestampTextCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/Infinity"

  fun apply(h: TestHelper) ? =>
    match _TimestampTextCodec.decode("infinity".array())?
    | let t: PgTimestamp =>
      h.assert_eq[I64](I64.max_value(), t.microseconds)
    else h.fail("Expected PgTimestamp from decode")
    end

    match _TimestampTextCodec.decode("-infinity".array())?
    | let t: PgTimestamp =>
      h.assert_eq[I64](I64.min_value(), t.microseconds)
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestampTextCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/Encode"

  fun apply(h: TestHelper) ? =>
    // Epoch
    let encoded = _TimestampTextCodec.encode(PgTimestamp(0))?
    h.assert_eq[String]("2000-01-01 00:00:00", String.from_array(encoded))

// ---------------------------------------------------------------------------
// TimestamptzTextCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestTimestamptzTextCodecDecodeUTC is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/DecodeUTC"

  fun apply(h: TestHelper) ? =>
    let codec = _TimestamptzTextCodec
    h.assert_eq[U16](0, codec.format())

    // 2024-01-15 14:30:00+00 — timezone stripped, same as timestamp
    match codec.decode("2024-01-15 14:30:00+00".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestamptzTextCodecDecodePositiveOffset is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/DecodePositiveOffset"

  fun apply(h: TestHelper) ? =>
    // 2024-01-15 14:30:00+05:30 — timezone suffix stripped, value kept as-is.
    // The resulting microseconds represent session-local time, not UTC.
    match _TimestamptzTextCodec.decode("2024-01-15 14:30:00+05:30".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestamptzTextCodecDecodeNegativeOffset is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/DecodeNegativeOffset"

  fun apply(h: TestHelper) ? =>
    // 2024-01-15 14:30:00-07
    match _TimestamptzTextCodec.decode("2024-01-15 14:30:00-07".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestamptzTextCodecDecodeFractionalWithTz is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/DecodeFractionalWithTz"

  fun apply(h: TestHelper) ? =>
    // 2024-01-15 14:30:00.123+00
    match _TimestamptzTextCodec.decode(
      "2024-01-15 14:30:00.123+00".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("2024-01-15 14:30:00.123", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestamptzTextCodecInfinity is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/Infinity"

  fun apply(h: TestHelper) ? =>
    match _TimestamptzTextCodec.decode("infinity".array())?
    | let t: PgTimestamp =>
      h.assert_eq[I64](I64.max_value(), t.microseconds)
    else h.fail("Expected PgTimestamp from decode")
    end

    match _TimestamptzTextCodec.decode("-infinity".array())?
    | let t: PgTimestamp =>
      h.assert_eq[I64](I64.min_value(), t.microseconds)
    else h.fail("Expected PgTimestamp from decode")
    end

class \nodoc\ iso _TestTimestamptzTextCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/Encode"

  fun apply(h: TestHelper) ? =>
    let ts = PgTimestamp(757_382_400_000_000)
    match _TimestamptzTextCodec.encode(ts)?
    | let encoded: Array[U8] val =>
      h.assert_eq[String]("2024-01-01 00:00:00", String.from_array(encoded))
    end

class \nodoc\ iso _TestTimestamptzTextCodecNegativeYear is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/NegativeYear"

  fun apply(h: TestHelper) ? =>
    // "-0001-06-15 12:00:00.123+05:30" -> strips tz, parses negative year
    match _TimestamptzTextCodec.decode(
      "-0001-06-15 12:00:00.123+05:30".array())?
    | let ts: PgTimestamp =>
      // Same microseconds as _TimestampTextCodec would produce for
      // "-0001-06-15 12:00:00.123"
      match _TimestampTextCodec.decode(
        "-0001-06-15 12:00:00.123".array())?
      | let expected: PgTimestamp =>
        h.assert_eq[I64](expected.microseconds, ts.microseconds)
      else h.fail("Expected PgTimestamp from timestamp decode")
      end
    else h.fail("Expected PgTimestamp from timestamptz decode")
    end

// ---------------------------------------------------------------------------
// IntervalTextCodec
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestIntervalTextCodecFullFormat is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/FullFormat"

  fun apply(h: TestHelper) ? =>
    let codec = _IntervalTextCodec
    h.assert_eq[U16](0, codec.format())

    // "1 year 2 mons 3 days 04:05:06"
    match codec.decode("1 year 2 mons 3 days 04:05:06".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](14, i.months)  // 1 year + 2 months
      h.assert_eq[I32](3, i.days)
      // 4*3600 + 5*60 + 6 = 14706 seconds = 14706000000 us
      h.assert_eq[I64](14_706_000_000, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecTimeOnly is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/TimeOnly"

  fun apply(h: TestHelper) ? =>
    // "01:00:00"
    match _IntervalTextCodec.decode("01:00:00".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](0, i.months)
      h.assert_eq[I32](0, i.days)
      h.assert_eq[I64](3_600_000_000, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecDaysOnly is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/DaysOnly"

  fun apply(h: TestHelper) ? =>
    // "5 days"
    match _IntervalTextCodec.decode("5 days".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](0, i.months)
      h.assert_eq[I32](5, i.days)
      h.assert_eq[I64](0, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecYearsOnly is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/YearsOnly"

  fun apply(h: TestHelper) ? =>
    // "2 years"
    match _IntervalTextCodec.decode("2 years".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](24, i.months)
      h.assert_eq[I32](0, i.days)
      h.assert_eq[I64](0, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecNegativeTime is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/NegativeTime"

  fun apply(h: TestHelper) ? =>
    // "1 day -01:00:00"
    match _IntervalTextCodec.decode("1 day -01:00:00".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](0, i.months)
      h.assert_eq[I32](1, i.days)
      h.assert_eq[I64](-3_600_000_000, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecNegativeDays is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/NegativeDays"

  fun apply(h: TestHelper) ? =>
    // "-3 days"
    match _IntervalTextCodec.decode("-3 days".array())?
    | let i: PgInterval =>
      h.assert_eq[I32](0, i.months)
      h.assert_eq[I32](-3, i.days)
      h.assert_eq[I64](0, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecFractionalSeconds is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/FractionalSeconds"

  fun apply(h: TestHelper) ? =>
    // "00:00:01.5"
    match _IntervalTextCodec.decode("00:00:01.5".array())?
    | let i: PgInterval =>
      h.assert_eq[I64](1_500_000, i.microseconds)
    else h.fail("Expected PgInterval from decode")
    end

class \nodoc\ iso _TestIntervalTextCodecEncode is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/Encode"

  fun apply(h: TestHelper) ? =>
    let interval = PgInterval(3_600_000_000, 5, 14)
    let encoded = _IntervalTextCodec.encode(interval)?
    h.assert_eq[String]("1 year 2 mons 5 days 01:00:00",
      String.from_array(encoded))

// ---------------------------------------------------------------------------
// Temporal type string() tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestPgTimestampString is UnitTest
  fun name(): String =>
    "Temporal/PgTimestamp/String"

  fun apply(h: TestHelper) =>
    // Epoch
    h.assert_eq[String]("2000-01-01 00:00:00", PgTimestamp(0).string())
    // Positive
    h.assert_eq[String]("2000-01-02 00:00:00",
      PgTimestamp(86_400_000_000).string())
    // Negative (before epoch)
    h.assert_eq[String]("1999-12-31 00:00:00",
      PgTimestamp(-86_400_000_000).string())
    // Infinity
    h.assert_eq[String]("infinity",
      PgTimestamp(I64.max_value()).string())
    h.assert_eq[String]("-infinity",
      PgTimestamp(I64.min_value()).string())
    // Fractional seconds
    h.assert_eq[String]("2000-01-01 00:00:00.5",
      PgTimestamp(500_000).string())
    h.assert_eq[String]("2000-01-01 00:00:00.123456",
      PgTimestamp(123_456).string())
    // Negative fractional (1 microsecond before epoch)
    h.assert_eq[String]("1999-12-31 23:59:59.999999",
      PgTimestamp(-1).string())
    // Negative fractional (half second before epoch)
    h.assert_eq[String]("1999-12-31 23:59:59.5",
      PgTimestamp(-500_000).string())

class \nodoc\ iso _TestPgDateString is UnitTest
  fun name(): String =>
    "Temporal/PgDate/String"

  fun apply(h: TestHelper) =>
    // Epoch
    h.assert_eq[String]("2000-01-01", PgDate(0).string())
    // Positive days
    h.assert_eq[String]("2000-01-02", PgDate(1).string())
    // Negative days
    h.assert_eq[String]("1999-12-31", PgDate(-1).string())
    // Infinity
    h.assert_eq[String]("infinity",
      PgDate(I32.max_value()).string())
    h.assert_eq[String]("-infinity",
      PgDate(I32.min_value()).string())

class \nodoc\ iso _TestPgTimeString is UnitTest
  fun name(): String =>
    "Temporal/PgTime/String"

  fun apply(h: TestHelper) ? =>
    // Midnight
    h.assert_eq[String]("00:00:00",
      PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds).string())
    // 14:30:00
    h.assert_eq[String]("14:30:00",
      PgTime(MakePgTimeMicroseconds(52_200_000_000)
        as PgTimeMicroseconds).string())
    // With fractional
    h.assert_eq[String]("14:30:00.123456",
      PgTime(MakePgTimeMicroseconds(52_200_123_456)
        as PgTimeMicroseconds).string())
    // Short fractional (trimmed trailing zeros)
    h.assert_eq[String]("14:30:00.5",
      PgTime(MakePgTimeMicroseconds(52_200_500_000)
        as PgTimeMicroseconds).string())

class \nodoc\ iso _TestPgTimeValidation is UnitTest
  """
  PgTimeValidator rejects values outside valid range [0, 86_400_000_000).
  """
  fun name(): String =>
    "Temporal/PgTime/Validation"

  fun apply(h: TestHelper) =>
    // Valid boundary: zero (midnight)
    match MakePgTimeMicroseconds(0)
    | let _: PgTimeMicroseconds => None
    | let _: ValidationFailure => h.fail("Expected valid for 0")
    end
    // Valid boundary: just before max (23:59:59.999999)
    match MakePgTimeMicroseconds(86_399_999_999)
    | let _: PgTimeMicroseconds => None
    | let _: ValidationFailure => h.fail("Expected valid for max-1")
    end
    // Invalid: negative
    match MakePgTimeMicroseconds(-1)
    | let _: PgTimeMicroseconds => h.fail("Expected failure for -1")
    | let _: ValidationFailure => None
    end
    // Invalid: at max (24:00:00 is not a valid time)
    match MakePgTimeMicroseconds(86_400_000_000)
    | let _: PgTimeMicroseconds =>
      h.fail("Expected failure for 86_400_000_000")
    | let _: ValidationFailure => None
    end
    // Invalid: well above max
    match MakePgTimeMicroseconds(I64.max_value())
    | let _: PgTimeMicroseconds =>
      h.fail("Expected failure for I64.max_value()")
    | let _: ValidationFailure => None
    end
    // Invalid: large negative
    match MakePgTimeMicroseconds(I64.min_value())
    | let _: PgTimeMicroseconds =>
      h.fail("Expected failure for I64.min_value()")
    | let _: ValidationFailure => None
    end

class \nodoc\ iso _TestPgIntervalString is UnitTest
  fun name(): String =>
    "Temporal/PgInterval/String"

  fun apply(h: TestHelper) =>
    // All components: 1 year 2 mons 3 days 04:05:06
    h.assert_eq[String]("1 year 2 mons 3 days 04:05:06",
      PgInterval(14_706_000_000, 3, 14).string())
    // Zero interval
    h.assert_eq[String]("00:00:00",
      PgInterval(0, 0, 0).string())
    // Negative time
    h.assert_eq[String]("1 day -01:00:00",
      PgInterval(-3_600_000_000, 1, 0).string())
    // Partial: only months
    h.assert_eq[String]("6 mons",
      PgInterval(0, 0, 6).string())
    // Partial: only days
    h.assert_eq[String]("10 days",
      PgInterval(0, 10, 0).string())
    // Singular forms
    h.assert_eq[String]("1 year",
      PgInterval(0, 0, 12).string())
    h.assert_eq[String]("1 mon",
      PgInterval(0, 0, 1).string())
    h.assert_eq[String]("1 day",
      PgInterval(0, 1, 0).string())
    // Fractional seconds in interval
    h.assert_eq[String]("00:00:01.5",
      PgInterval(1_500_000, 0, 0).string())
    // Plural forms
    h.assert_eq[String]("2 years",
      PgInterval(0, 0, 24).string())
    h.assert_eq[String]("3 mons",
      PgInterval(0, 0, 3).string())
    h.assert_eq[String]("5 days",
      PgInterval(0, 5, 0).string())

// ---------------------------------------------------------------------------
// Field.eq temporal tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestFieldEqualityTemporal is UnitTest
  fun name(): String =>
    "Field/Equality/Temporal"

  fun apply(h: TestHelper) ? =>
    // PgTimestamp: same-type equal
    h.assert_true(
      Field("a", PgTimestamp(1000)) == Field("a", PgTimestamp(1000)))
    // PgTimestamp: same-type not-equal
    h.assert_false(
      Field("a", PgTimestamp(1000)) == Field("a", PgTimestamp(2000)))

    // PgDate: same-type equal
    h.assert_true(
      Field("a", PgDate(100)) == Field("a", PgDate(100)))
    // PgDate: same-type not-equal
    h.assert_false(
      Field("a", PgDate(100)) == Field("a", PgDate(200)))

    // PgTime: same-type equal
    h.assert_true(
      Field("a",
        PgTime(MakePgTimeMicroseconds(5000) as PgTimeMicroseconds))
        == Field("a",
        PgTime(MakePgTimeMicroseconds(5000) as PgTimeMicroseconds)))
    // PgTime: same-type not-equal
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(5000) as PgTimeMicroseconds))
        == Field("a",
        PgTime(MakePgTimeMicroseconds(6000) as PgTimeMicroseconds)))

    // PgInterval: same-type equal
    h.assert_true(
      Field("a", PgInterval(100, 2, 3))
        == Field("a", PgInterval(100, 2, 3)))
    // PgInterval: same-type not-equal (differs in each component)
    h.assert_false(
      Field("a", PgInterval(100, 2, 3))
        == Field("a", PgInterval(200, 2, 3)))
    h.assert_false(
      Field("a", PgInterval(100, 2, 3))
        == Field("a", PgInterval(100, 5, 3)))
    h.assert_false(
      Field("a", PgInterval(100, 2, 3))
        == Field("a", PgInterval(100, 2, 7)))

class \nodoc\ iso _TestFieldInequalityCrossTypeTemporal is UnitTest
  fun name(): String =>
    "Field/Inequality/CrossTypeTemporal"

  fun apply(h: TestHelper) ? =>
    // PgTimestamp vs I64 with same numeric value: different types, not equal
    h.assert_false(
      Field("a", PgTimestamp(1000)) == Field("a", I64(1000)))
    // PgDate vs I32 with same numeric value
    h.assert_false(
      Field("a", PgDate(100)) == Field("a", I32(100)))
    // PgTime vs I64 with same numeric value
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(5000) as PgTimeMicroseconds))
        == Field("a", I64(5000)))
    // PgTimestamp vs PgDate: different temporal types
    h.assert_false(
      Field("a", PgTimestamp(0)) == Field("a", PgDate(0)))
    // PgTime vs PgTimestamp
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds))
        == Field("a", PgTimestamp(0)))

// ---------------------------------------------------------------------------
// RowsBuilder tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestRowsBuilderBinaryFormat is UnitTest
  fun name(): String =>
    "RowsBuilder/BinaryFormat"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry

    // Simulate a row with binary-format columns:
    // bool (OID 16, format 1), int4 (OID 23, format 1), text (OID 25, format 1)
    let row_descs: Array[(String, U32, U16)] val = recover val
      [("flag", 16, 1); ("count", 23, 1); ("name", 25, 1)]
    end

    // bool true = [1], int4 42 = big-endian [0;0;0;42], text "hi" = [104;105]
    let bool_data: Array[U8] val = recover val [1] end
    let int_data: Array[U8] val = recover val [0; 0; 0; 42] end
    let text_data: Array[U8] val = "hi".array()

    let raw_rows: Array[Array[(Array[U8] val | None)] val] val = recover val
      let row: Array[(Array[U8] val | None)] iso =
        recover iso
          let r = Array[(Array[U8] val | None)]
          r.push(bool_data)
          r.push(int_data)
          r.push(text_data)
          r
        end
      [consume row]
    end

    let rows = _RowsBuilder(raw_rows, row_descs, reg)?
    h.assert_eq[USize](1, rows.size())
    let row = rows(0)?
    h.assert_eq[USize](3, row.fields.size())

    match row.fields(0)?.value
    | let v: Bool => h.assert_true(v)
    else h.fail("Expected Bool for flag")
    end

    match row.fields(1)?.value
    | let v: I32 => h.assert_eq[I32](42, v)
    else h.fail("Expected I32 for count")
    end

    match row.fields(2)?.value
    | let v: String => h.assert_eq[String]("hi", v)
    else h.fail("Expected String for name")
    end

class \nodoc\ iso _TestRowsBuilderTextFormat is UnitTest
  fun name(): String =>
    "RowsBuilder/TextFormat"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry

    // Simulate text-format columns:
    // bool (OID 16, format 0), int4 (OID 23, format 0), date (OID 1082, format 0)
    let row_descs: Array[(String, U32, U16)] val = recover val
      [("flag", 16, 0); ("count", 23, 0); ("dt", 1082, 0)]
    end

    let bool_data: Array[U8] val = "t".array()
    let int_data: Array[U8] val = "42".array()
    let date_data: Array[U8] val = "2024-01-15".array()

    let raw_rows: Array[Array[(Array[U8] val | None)] val] val = recover val
      let row: Array[(Array[U8] val | None)] iso =
        recover iso
          let r = Array[(Array[U8] val | None)]
          r.push(bool_data)
          r.push(int_data)
          r.push(date_data)
          r
        end
      [consume row]
    end

    let rows = _RowsBuilder(raw_rows, row_descs, reg)?
    h.assert_eq[USize](1, rows.size())
    let row = rows(0)?

    match row.fields(0)?.value
    | let v: Bool => h.assert_true(v)
    else h.fail("Expected Bool for flag")
    end

    match row.fields(1)?.value
    | let v: I32 => h.assert_eq[I32](42, v)
    else h.fail("Expected I32 for count")
    end

    match row.fields(2)?.value
    | let v: PgDate =>
      h.assert_eq[String]("2024-01-15", v.string())
    else h.fail("Expected PgDate for dt")
    end

class \nodoc\ iso _TestRowsBuilderNullHandling is UnitTest
  fun name(): String =>
    "RowsBuilder/NullHandling"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry

    let row_descs: Array[(String, U32, U16)] val = recover val
      [("val", 23, 1)]
    end

    let raw_rows: Array[Array[(Array[U8] val | None)] val] val = recover val
      let row: Array[(Array[U8] val | None)] iso =
        recover iso
          let r = Array[(Array[U8] val | None)]
          r.push(None)
          r
        end
      [consume row]
    end

    let rows = _RowsBuilder(raw_rows, row_descs, reg)?
    match rows(0)?.fields(0)?.value
    | None => None
    else h.fail("Expected None for NULL column")
    end

class \nodoc\ iso _TestRowsBuilderBinaryTemporalTypes is UnitTest
  fun name(): String =>
    "RowsBuilder/BinaryTemporalTypes"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry

    // timestamp (OID 1114, format 1), date (OID 1082, format 1),
    // time (OID 1083, format 1), interval (OID 1186, format 1)
    let row_descs: Array[(String, U32, U16)] val = recover val
      [("ts", 1114, 1); ("dt", 1082, 1); ("tm", 1083, 1); ("iv", 1186, 1)]
    end

    // Encode PgTimestamp(0), PgDate(0), PgTime(0), PgInterval(0,0,0)
    // All zeros in big-endian
    let ts_data: Array[U8] val = recover val Array[U8].init(0, 8) end
    let dt_data: Array[U8] val = recover val Array[U8].init(0, 4) end
    let tm_data: Array[U8] val = recover val Array[U8].init(0, 8) end
    let iv_data: Array[U8] val = recover val Array[U8].init(0, 16) end

    let raw_rows: Array[Array[(Array[U8] val | None)] val] val = recover val
      let row: Array[(Array[U8] val | None)] iso =
        recover iso
          let r = Array[(Array[U8] val | None)]
          r.push(ts_data)
          r.push(dt_data)
          r.push(tm_data)
          r.push(iv_data)
          r
        end
      [consume row]
    end

    let rows = _RowsBuilder(raw_rows, row_descs, reg)?
    let row = rows(0)?

    match row.fields(0)?.value
    | let v: PgTimestamp => h.assert_true(PgTimestamp(0) == v)
    else h.fail("Expected PgTimestamp for ts")
    end

    match row.fields(1)?.value
    | let v: PgDate => h.assert_true(PgDate(0) == v)
    else h.fail("Expected PgDate for dt")
    end

    match row.fields(2)?.value
    | let v: PgTime =>
      h.assert_true(
        PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds) == v)
    else h.fail("Expected PgTime for tm")
    end

    match row.fields(3)?.value
    | let v: PgInterval => h.assert_true(PgInterval(0, 0, 0) == v)
    else h.fail("Expected PgInterval for iv")
    end

// ---------------------------------------------------------------------------
// CodecRegistry tests for new types
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestCodecRegistryDecodeBinaryDate is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryDate"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1082 (date), binary format, 0 days = epoch
    let data: Array[U8] val = recover val [0; 0; 0; 0] end
    match reg.decode(1082, 1, data)?
    | let d: PgDate => h.assert_eq[I32](0, d.days)
    else h.fail("Expected PgDate from binary date decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryTimestamp is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryTimestamp"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1114 (timestamp), binary format, 0 us = epoch
    let data: Array[U8] val = recover val Array[U8].init(0, 8) end
    match reg.decode(1114, 1, data)?
    | let t: PgTimestamp => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTimestamp from binary timestamp decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryTimestamptz is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryTimestamptz"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1184 (timestamptz) uses same binary codec as timestamp
    let data: Array[U8] val = recover val Array[U8].init(0, 8) end
    match reg.decode(1184, 1, data)?
    | let t: PgTimestamp => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTimestamp from binary timestamptz decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeTextDate is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/TextDate"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1082 (date), text format
    let data: Array[U8] val = "2000-01-01".array()
    match reg.decode(1082, 0, data)?
    | let d: PgDate => h.assert_eq[I32](0, d.days)
    else h.fail("Expected PgDate from text date decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeTextTimestamptz is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/TextTimestamptz"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1184 (timestamptz), text format with timezone
    let data: Array[U8] val = "2000-01-01 00:00:00+00".array()
    match reg.decode(1184, 0, data)?
    | let t: PgTimestamp => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTimestamp from text timestamptz decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeTextInterval is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/TextInterval"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1186 (interval), text format
    let data: Array[U8] val = "1 day".array()
    match reg.decode(1186, 0, data)?
    | let i: PgInterval =>
      h.assert_eq[I32](1, i.days)
      h.assert_eq[I32](0, i.months)
      h.assert_eq[I64](0, i.microseconds)
    else h.fail("Expected PgInterval from text interval decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryUuid is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryUuid"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 2950 (uuid), binary format
    let data: Array[U8] val = recover val Array[U8].init(0, 16) end
    match reg.decode(2950, 1, data)?
    | let s: String =>
      h.assert_eq[String]("00000000-0000-0000-0000-000000000000", s)
    else h.fail("Expected String from binary uuid decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryJsonb is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryJsonb"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 3802 (jsonb), binary format: version byte + JSON
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(1)
      a.append("{}".array())
      a
    end
    match reg.decode(3802, 1, data)?
    | let s: String => h.assert_eq[String]("{}", s)
    else h.fail("Expected String from binary jsonb decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryOid is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryOid"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 26 (oid), binary format: 4 bytes big-endian for value 12345
    let data: Array[U8] val = recover val [0; 0; 0x30; 0x39] end
    match reg.decode(26, 1, data)?
    | let s: String => h.assert_eq[String]("12345", s)
    else h.fail("Expected String from binary oid decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryTextPassthrough is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryTextPassthrough"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 25 (text), binary format: raw UTF-8
    let data: Array[U8] val = "hello".array()
    match reg.decode(25, 1, data)?
    | let s: String => h.assert_eq[String]("hello", s)
    else h.fail("Expected String from binary text decode")
    end

    // OID 1043 (varchar), binary format
    let varchar_data: Array[U8] val = "world".array()
    match reg.decode(1043, 1, varchar_data)?
    | let s: String => h.assert_eq[String]("world", s)
    else h.fail("Expected String from binary varchar decode")
    end

// ---------------------------------------------------------------------------
// Text codec direct tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestInt2TextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Int2/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int2TextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode(I16(42))?
    h.assert_array_eq[U8]("42".array(), encoded)
    match codec.decode(encoded)?
    | let decoded: I16 => h.assert_eq[I16](42, decoded)
    else h.fail("Expected I16 from decode")
    end

    // Negative
    let encoded_neg = codec.encode(I16(-1))?
    h.assert_array_eq[U8]("-1".array(), encoded_neg)
    match codec.decode(encoded_neg)?
    | let decoded: I16 => h.assert_eq[I16](-1, decoded)
    else h.fail("Expected I16 from decode")
    end

class \nodoc\ iso _TestInt8TextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Int8/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Int8TextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode(I64(9999999999))?
    h.assert_array_eq[U8]("9999999999".array(), encoded)
    match codec.decode(encoded)?
    | let decoded: I64 => h.assert_eq[I64](9999999999, decoded)
    else h.fail("Expected I64 from decode")
    end

class \nodoc\ iso _TestFloat4TextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Float4/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Float4TextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode(F32(1.5))?
    match codec.decode(encoded)?
    | let decoded: F32 => h.assert_eq[F32](F32(1.5), decoded)
    else h.fail("Expected F32 from decode")
    end

class \nodoc\ iso _TestFloat8TextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Float8/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _Float8TextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode(F64(3.14))?
    match codec.decode(encoded)?
    | let decoded: F64 => h.assert_eq[F64](F64(3.14), decoded)
    else h.fail("Expected F64 from decode")
    end

class \nodoc\ iso _TestTextPassthroughTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/TextPassthrough/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _TextPassthroughTextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode("hello world")?
    h.assert_array_eq[U8]("hello world".array(), encoded)
    match codec.decode(encoded)
    | let s: String => h.assert_eq[String]("hello world", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestOidTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Oid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _OidTextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode("12345")?
    h.assert_array_eq[U8]("12345".array(), encoded)
    match codec.decode(encoded)
    | let s: String => h.assert_eq[String]("12345", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestNumericTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Numeric/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _NumericTextCodec
    h.assert_eq[U16](0, codec.format())

    let encoded = codec.encode("3.14")?
    h.assert_array_eq[U8]("3.14".array(), encoded)
    match codec.decode(encoded)
    | let s: String => h.assert_eq[String]("3.14", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestUuidTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Uuid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _UuidTextCodec
    h.assert_eq[U16](0, codec.format())

    let uuid = "550e8400-e29b-41d4-a716-446655440000"
    let encoded = codec.encode(uuid)?
    h.assert_array_eq[U8](uuid.array(), encoded)
    match codec.decode(encoded)
    | let s: String => h.assert_eq[String](uuid, s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestJsonbTextCodecRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Text/Jsonb/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let codec = _JsonbTextCodec
    h.assert_eq[U16](0, codec.format())

    let json = """{"key": "value"}"""
    let encoded = codec.encode(json)?
    h.assert_array_eq[U8](json.array(), encoded)
    match codec.decode(encoded)
    | let s: String => h.assert_eq[String](json, s)
    else h.fail("Expected String from decode")
    end

// ---------------------------------------------------------------------------
// Negative year tests for temporal text codecs
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestDateTextCodecNegativeYear is UnitTest
  fun name(): String =>
    "Codec/Text/Date/NegativeYear"

  fun apply(h: TestHelper) ? =>
    // PostgreSQL represents BC dates as negative years: -0001-01-01
    match _DateTextCodec.decode("-0001-01-01".array())?
    | let d: PgDate =>
      h.assert_eq[String]("-0001-01-01", d.string())
    else h.fail("Expected PgDate from decode")
    end

    // 0001-01-01 (year 1 AD, positive) for comparison
    match _DateTextCodec.decode("0001-01-01".array())?
    | let d: PgDate =>
      h.assert_eq[String]("0001-01-01", d.string())
    else h.fail("Expected PgDate from decode")
    end

// ---------------------------------------------------------------------------
// Text codec type mismatch tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestDateTextCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Date/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _DateTextCodec.encode("2024-01-15")? })

class \nodoc\ iso _TestTimeTextCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Time/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _TimeTextCodec.encode("14:30:00")? })

class \nodoc\ iso _TestTimestampTextCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _TimestampTextCodec.encode("2024-01-15 14:30:00")? })

class \nodoc\ iso _TestTimestamptzTextCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error(
      {()? => _TimestamptzTextCodec.encode("2024-01-15 14:30:00+00")? })

class \nodoc\ iso _TestIntervalTextCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _IntervalTextCodec.encode("1 day")? })

// ---------------------------------------------------------------------------
// Text codec decode error-path tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestDateTextCodecBadInput is UnitTest
  fun name(): String =>
    "Codec/Text/Date/BadInput"

  fun apply(h: TestHelper) =>
    // Not a date at all
    h.assert_error({()? => _DateTextCodec.decode("not-a-date".array())? })
    // Missing day
    h.assert_error({()? => _DateTextCodec.decode("2024-01".array())? })

class \nodoc\ iso _TestTimeTextCodecBadInput is UnitTest
  fun name(): String =>
    "Codec/Text/Time/BadInput"

  fun apply(h: TestHelper) =>
    // Not a time
    h.assert_error({()? => _TimeTextCodec.decode("not-a-time".array())? })
    // Missing seconds
    h.assert_error({()? => _TimeTextCodec.decode("14:30".array())? })

class \nodoc\ iso _TestTimeTextCodecOutOfRange is UnitTest
  """
  Parseable but out-of-range time text is rejected by PgTimeValidator.
  """
  fun name(): String =>
    "Codec/Text/Time/OutOfRange"

  fun apply(h: TestHelper) =>
    // 25:00:00 = 90,000,000,000 us, outside [0, 86,400,000,000)
    h.assert_error({()? => _TimeTextCodec.decode("25:00:00".array())? })

class \nodoc\ iso _TestTimestampTextCodecBadInput is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/BadInput"

  fun apply(h: TestHelper) =>
    // No space separator
    h.assert_error(
      {()? => _TimestampTextCodec.decode("not-a-timestamp".array())? })
    // Missing time
    h.assert_error(
      {()? => _TimestampTextCodec.decode("2024-01-15".array())? })

class \nodoc\ iso _TestTimestamptzTextCodecBadInput is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamptz/BadInput"

  fun apply(h: TestHelper) =>
    // No space separator
    h.assert_error(
      {()? => _TimestamptzTextCodec.decode("not-a-timestamp".array())? })

class \nodoc\ iso _TestIntervalTextCodecBadInput is UnitTest
  fun name(): String =>
    "Codec/Text/Interval/BadInput"

  fun apply(h: TestHelper) =>
    // Number without unit pair
    h.assert_error({()? => _IntervalTextCodec.decode("5".array())? })

// ---------------------------------------------------------------------------
// CodecRegistry dispatch tests for remaining temporal types
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestCodecRegistryDecodeBinaryTime is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryTime"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1083 (time), binary format, 0 us = midnight
    let data: Array[U8] val = recover val Array[U8].init(0, 8) end
    match reg.decode(1083, 1, data)?
    | let t: PgTime => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTime from binary time decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeBinaryInterval is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/BinaryInterval"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1186 (interval), binary format, all zeros
    let data: Array[U8] val = recover val Array[U8].init(0, 16) end
    match reg.decode(1186, 1, data)?
    | let i: PgInterval =>
      h.assert_eq[I64](0, i.microseconds)
      h.assert_eq[I32](0, i.days)
      h.assert_eq[I32](0, i.months)
    else h.fail("Expected PgInterval from binary interval decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeTextTimestamp is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/TextTimestamp"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1114 (timestamp), text format
    let data: Array[U8] val = "2000-01-01 00:00:00".array()
    match reg.decode(1114, 0, data)?
    | let t: PgTimestamp => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTimestamp from text timestamp decode")
    end

class \nodoc\ iso _TestCodecRegistryDecodeTextTime is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/TextTime"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry
    // OID 1083 (time), text format
    let data: Array[U8] val = "00:00:00".array()
    match reg.decode(1083, 0, data)?
    | let t: PgTime => h.assert_eq[I64](0, t.microseconds)
    else h.fail("Expected PgTime from text time decode")
    end

class \nodoc\ iso _TestTimestampTextCodecNegativeYear is UnitTest
  fun name(): String =>
    "Codec/Text/Timestamp/NegativeYear"

  fun apply(h: TestHelper) ? =>
    // BC timestamp: -0001-06-15 12:00:00
    match _TimestampTextCodec.decode("-0001-06-15 12:00:00".array())?
    | let t: PgTimestamp =>
      h.assert_eq[String]("-0001-06-15 12:00:00", t.string())
    else h.fail("Expected PgTimestamp from decode")
    end

// ---------------------------------------------------------------------------
// Gap-fill tests identified by ensemble review
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestByteaBinaryCodecTypeMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Bytea/TypeMismatch"

  fun apply(h: TestHelper) =>
    h.assert_error({()? => _ByteaBinaryCodec.encode("not bytes")? })
    h.assert_error({()? => _ByteaBinaryCodec.encode(I32(42))? })

class \nodoc\ iso _TestTimeBinaryCodecOutOfRange is UnitTest
  fun name(): String =>
    "Codec/Binary/Time/OutOfRange"

  fun apply(h: TestHelper) =>
    // 86_400_000_000 us (exactly 24h) = 0x00000014_1DD76000 big-endian.
    // Out of PgTime's valid range; decode rejects via PgTimeValidator.
    let at_boundary: Array[U8] val = recover val
      [0x00; 0x00; 0x00; 0x14; 0x1D; 0xD7; 0x60; 0x00]
    end
    h.assert_error({()? => _TimeBinaryCodec.decode(at_boundary)? })

    // Negative microseconds (I64(-1) = 0xFFFFFFFF_FFFFFFFF big-endian):
    // also invalid for time-of-day
    let negative: Array[U8] val = recover val
      [0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF; 0xFF]
    end
    h.assert_error({()? => _TimeBinaryCodec.decode(negative)? })

class \nodoc\ iso _TestFloat4BinaryCodecNaN is UnitTest
  fun name(): String =>
    "Codec/Binary/Float4/NaN"

  fun apply(h: TestHelper) ? =>
    let nan = F32(0) / F32(0)
    let encoded = _Float4BinaryCodec.encode(nan)?
    h.assert_eq[USize](4, encoded.size())
    match _Float4BinaryCodec.decode(encoded)?
    | let decoded: F32 =>
      // Pony: NaN == NaN is true (reflexive Equatable)
      h.assert_eq[F32](nan, decoded)
    else h.fail("Expected F32 from decode")
    end

class \nodoc\ iso _TestFloat8BinaryCodecNaN is UnitTest
  fun name(): String =>
    "Codec/Binary/Float8/NaN"

  fun apply(h: TestHelper) ? =>
    let nan = F64(0) / F64(0)
    let encoded = _Float8BinaryCodec.encode(nan)?
    h.assert_eq[USize](8, encoded.size())
    match _Float8BinaryCodec.decode(encoded)?
    | let decoded: F64 =>
      // Pony: NaN == NaN is true (reflexive Equatable)
      h.assert_eq[F64](nan, decoded)
    else h.fail("Expected F64 from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecLargeNumber is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/LargeNumber"

  fun apply(h: TestHelper) ? =>
    // 99999999.99999999: ndigits=4, weight=1, sign=0x0000, dscale=8
    // base-10000 digits: 9999, 9999, 9999, 9999
    // Integer part: weight=1 means digits[0]*10000 + digits[1] = 99999999
    // Fractional part: dscale=8, digits[2]=9999 (4 digits), digits[3]=9999
    let data: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(4)    // ndigits = 4
      a.push(0); a.push(1)    // weight = 1
      a.push(0); a.push(0)    // sign = 0x0000 (positive)
      a.push(0); a.push(8)    // dscale = 8
      a.push(0x27); a.push(0x0F)  // digit[0] = 9999
      a.push(0x27); a.push(0x0F)  // digit[1] = 9999
      a.push(0x27); a.push(0x0F)  // digit[2] = 9999
      a.push(0x27); a.push(0x0F)  // digit[3] = 9999
      a
    end
    match _NumericBinaryCodec.decode(data)?
    | let s: String => h.assert_eq[String]("99999999.99999999", s)
    else h.fail("Expected String from decode")
    end

    // Large integer with many digit groups: 1000000000000 (1 trillion)
    // ndigits=4, weight=3, sign=0x0000, dscale=0
    // digits: 1, 0, 0, 0
    let trillion: Array[U8] val = recover val
      let a = Array[U8]
      a.push(0); a.push(4)    // ndigits = 4
      a.push(0); a.push(3)    // weight = 3
      a.push(0); a.push(0)    // sign = 0x0000
      a.push(0); a.push(0)    // dscale = 0
      a.push(0); a.push(1)    // digit[0] = 1
      a.push(0); a.push(0)    // digit[1] = 0
      a.push(0); a.push(0)    // digit[2] = 0
      a.push(0); a.push(0)    // digit[3] = 0
      a
    end
    match _NumericBinaryCodec.decode(trillion)?
    | let s: String => h.assert_eq[String]("1000000000000", s)
    else h.fail("Expected String from decode")
    end

class \nodoc\ iso _TestNumericBinaryCodecNegativeNdigits is UnitTest
  """
  Numeric with negative ndigits is rejected rather than relying on
  overflow arithmetic from the I16-to-USize conversion.
  """
  fun name(): String =>
    "Codec/Binary/Numeric/NegativeNdigits"

  fun apply(h: TestHelper) =>
    // Big-endian wire format: ndigits=-1, weight=0, sign=0x0000, dscale=0
    let data: Array[U8] val = recover val
      [0xFF; 0xFF  // ndigits = -1
       0x00; 0x00  // weight = 0
       0x00; 0x00  // sign = positive
       0x00; 0x00] // dscale = 0
    end
    h.assert_error({()? => _NumericBinaryCodec.decode(data)? })

class \nodoc\ iso _TestPgIntervalStringMinValue is UnitTest
  fun name(): String =>
    "Temporal/PgInterval/String/MinValue"

  fun apply(h: TestHelper) =>
    // I64.min_value() microseconds should not produce garbage output.
    // The negation guard clamps to I64.max_value(), losing 1us.
    let interval = PgInterval(I64.min_value(), 0, 0)
    let s: String val = interval.string()
    // Should start with "-" (negative time) and contain valid HH:MM:SS
    h.assert_true(s.size() > 0)
    h.assert_true(s.at("-", 0))
    // Verify it matches what I64.max_value() microseconds would produce
    // (since we clamp min to max)
    let max_interval = PgInterval(I64.max_value(), 0, 0)
    let max_s: String val = max_interval.string()
    // The min_value version should be "-" + max_value's string
    h.assert_eq[String]("-" + consume max_s, s)

// ---------------------------------------------------------------------------
// Wrapper type tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestByteaString is UnitTest
  fun name(): String =>
    "Bytea/String"

  fun apply(h: TestHelper) =>
    let b = Bytea(recover val [as U8: 0xDE; 0xAD; 0xBE; 0xEF] end)
    h.assert_eq[String]("\\xdeadbeef", b.string())

    let empty = Bytea(recover val Array[U8] end)
    h.assert_eq[String]("\\x", empty.string())

class \nodoc\ iso _TestByteaEquality is UnitTest
  fun name(): String =>
    "Bytea/Equality"

  fun apply(h: TestHelper) =>
    let a = Bytea(recover val [as U8: 1; 2; 3] end)
    let b = Bytea(recover val [as U8: 1; 2; 3] end)
    h.assert_true(a == b)

    // Different content
    let c = Bytea(recover val [as U8: 1; 2; 4] end)
    h.assert_false(a == c)

    // Different lengths
    let d = Bytea(recover val [as U8: 1; 2] end)
    h.assert_false(a == d)

    // Empty
    let e = Bytea(recover val Array[U8] end)
    let f = Bytea(recover val Array[U8] end)
    h.assert_true(e == f)

class \nodoc\ iso _TestRawBytesString is UnitTest
  fun name(): String =>
    "RawBytes/String"

  fun apply(h: TestHelper) =>
    let r = RawBytes(recover val [as U8: 0xCA; 0xFE] end)
    h.assert_eq[String]("\\xcafe", r.string())

    let empty = RawBytes(recover val Array[U8] end)
    h.assert_eq[String]("\\x", empty.string())

class \nodoc\ iso _TestRawBytesEquality is UnitTest
  fun name(): String =>
    "RawBytes/Equality"

  fun apply(h: TestHelper) =>
    let a = RawBytes(recover val [as U8: 1; 2; 3] end)
    let b = RawBytes(recover val [as U8: 1; 2; 3] end)
    h.assert_true(a == b)

    let c = RawBytes(recover val [as U8: 1; 2; 4] end)
    h.assert_false(a == c)

    let d = RawBytes(recover val [as U8: 1; 2] end)
    h.assert_false(a == d)

    let e = RawBytes(recover val Array[U8] end)
    let f = RawBytes(recover val Array[U8] end)
    h.assert_true(e == f)

// ---------------------------------------------------------------------------
// Custom codec tests
// ---------------------------------------------------------------------------

class \nodoc\ val _TestPoint is (FieldData & FieldDataEquatable & Equatable[_TestPoint])
  let x: F64
  let y: F64

  new val create(x': F64, y': F64) =>
    x = x'
    y = y'

  fun string(): String iso^ =>
    recover iso
      let s = String
      s.append("(")
      s.append(x.string())
      s.append(",")
      s.append(y.string())
      s.append(")")
      s
    end

  fun eq(that: box->_TestPoint): Bool =>
    (x == that.x) and (y == that.y)

  fun field_data_eq(that: FieldData box): Bool =>
    match that
    | let p: _TestPoint box => (x == p.x) and (y == p.y)
    else false
    end

primitive \nodoc\ _TestPointCodec is Codec
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    error

  fun decode(data: Array[U8] val): FieldData ? =>
    if data.size() != 16 then error end
    let x = ifdef bigendian then
      F64.from_bits(data.read_u64(0)?)
    else
      F64.from_bits(data.read_u64(0)?.bswap())
    end
    let y = ifdef bigendian then
      F64.from_bits(data.read_u64(8)?)
    else
      F64.from_bits(data.read_u64(8)?.bswap())
    end
    _TestPoint(x, y)

class \nodoc\ iso _TestCodecRegistryWithCodecBinary is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/Binary"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry.with_codec(600, _TestPointCodec)
    let data: Array[U8] val = recover val Array[U8].init(0, 16) end
    match reg.decode(600, 1, data)?
    | let p: _TestPoint =>
      h.assert_eq[F64](0, p.x)
      h.assert_eq[F64](0, p.y)
    else h.fail("Expected _TestPoint from custom codec")
    end

class \nodoc\ iso _TestCodecRegistryWithCodecText is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/Text"

  fun apply(h: TestHelper) ? =>
    // Text codec for same OID should fall back to String
    let reg = CodecRegistry.with_codec(600, _TestPointCodec)
    let data: Array[U8] val = "(1,2)".array()
    match reg.decode(600, 0, data)?
    | let s: String => h.assert_eq[String]("(1,2)", s)
    else h.fail("Expected String fallback for text format of custom binary codec")
    end

class \nodoc\ iso _TestCodecRegistryWithCodecOverride is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/Override"

  fun apply(h: TestHelper) =>
    // Override built-in bool codec with point codec
    let reg = CodecRegistry.with_codec(16, _TestPointCodec)
    // OID 16 is normally bool; with override, the point codec expects
    // 16 bytes, not 1 — decode error propagates to the caller
    let data: Array[U8] val = recover val [1] end
    h.assert_error({()? => reg.decode(16, 1, data)? })

class \nodoc\ iso _TestCodecRegistryWithCodecChaining is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/Chaining"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
      .with_codec(600, _TestPointCodec)
      .with_codec(601, _TestPointCodec)
    h.assert_true(reg.has_binary_codec(600))
    h.assert_true(reg.has_binary_codec(601))
    // Built-in codecs should still be present
    h.assert_true(reg.has_binary_codec(16))

class \nodoc\ iso _TestCodecRegistryWithCodecPreservesBuiltins is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/PreservesBuiltins"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry.with_codec(600, _TestPointCodec)
    // Verify built-in codecs still work
    let bool_data: Array[U8] val = recover val [1] end
    match reg.decode(16, 1, bool_data)?
    | let v: Bool => h.assert_true(v)
    else h.fail("Expected Bool from built-in codec after adding custom codec")
    end

class \nodoc\ iso _TestRowsBuilderWithCustomCodec is UnitTest
  fun name(): String =>
    "RowsBuilder/WithCustomCodec"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry.with_codec(600, _TestPointCodec)

    let row_descs: Array[(String, U32, U16)] val = recover val
      [("pt", 600, 1)]
    end

    let point_data: Array[U8] val = recover val Array[U8].init(0, 16) end

    let raw_rows: Array[Array[(Array[U8] val | None)] val] val = recover val
      let row: Array[(Array[U8] val | None)] iso =
        recover iso
          let r = Array[(Array[U8] val | None)]
          r.push(point_data)
          r
        end
      [consume row]
    end

    let rows = _RowsBuilder(raw_rows, row_descs, reg)?
    h.assert_eq[USize](1, rows.size())
    match rows(0)?.fields(0)?.value
    | let p: _TestPoint =>
      h.assert_eq[F64](0, p.x)
      h.assert_eq[F64](0, p.y)
    else h.fail("Expected _TestPoint from RowsBuilder with custom codec")
    end

// ---------------------------------------------------------------------------
// Custom equality tests
// ---------------------------------------------------------------------------

class \nodoc\ iso _TestFieldEqualityCustomType is UnitTest
  fun name(): String =>
    "Field/Equality/CustomType"

  fun apply(h: TestHelper) =>
    let p1 = _TestPoint(1.0, 2.0)
    let p2 = _TestPoint(1.0, 2.0)
    h.assert_true(Field("pt", p1) == Field("pt", p2))

class \nodoc\ iso _TestFieldInequalityCustomType is UnitTest
  fun name(): String =>
    "Field/Inequality/CustomType"

  fun apply(h: TestHelper) =>
    let p1 = _TestPoint(1.0, 2.0)
    let p2 = _TestPoint(3.0, 4.0)
    h.assert_false(Field("pt", p1) == Field("pt", p2))

class \nodoc\ val _TestOpaqueData is FieldData
  """
  Custom `FieldData` that does NOT implement `FieldDataEquatable`.
  Used to test that custom types without opt-in equality are never equal.
  """
  let tag_value: String

  new val create(tag_value': String) =>
    tag_value = tag_value'

  fun string(): String iso^ =>
    tag_value.clone()

class \nodoc\ iso _TestFieldEqualityCustomWithoutEquatable is UnitTest
  fun name(): String =>
    "Field/Equality/CustomWithoutEquatable"

  fun apply(h: TestHelper) =>
    // Two identical custom values without FieldDataEquatable are never equal
    let a = _TestOpaqueData("same")
    let b = _TestOpaqueData("same")
    h.assert_false(Field("x", a) == Field("x", b))
    // Same instance is also not equal (no FieldDataEquatable to dispatch to)
    h.assert_false(Field("x", a) == Field("x", a))

class \nodoc\ iso _TestFieldEqualityCustomVsBuiltin is UnitTest
  fun name(): String =>
    "Field/Equality/CustomVsBuiltin"

  fun apply(h: TestHelper) =>
    let p = _TestPoint(1.0, 2.0)
    h.assert_false(Field("x", p) == Field("x", I32(42)))
    h.assert_false(Field("x", I32(42)) == Field("x", p))

class \nodoc\ iso _TestFieldEqualityCustomEquatableVsNonEquatable is UnitTest
  """
  Verifies symmetry when comparing a custom type with FieldDataEquatable
  against a custom type without it. Neither direction should be equal.
  """
  fun name(): String =>
    "Field/Equality/CustomEquatableVsNonEquatable"

  fun apply(h: TestHelper) =>
    let equatable = _TestPoint(1.0, 2.0)
    let opaque = _TestOpaqueData("(1.0,2.0)")
    // FieldDataEquatable dispatches to _TestPoint.field_data_eq which returns
    // false for non-_TestPoint types
    h.assert_false(Field("x", equatable) == Field("x", opaque))
    // _TestOpaqueData has no FieldDataEquatable, falls to else false
    h.assert_false(Field("x", opaque) == Field("x", equatable))

// ---------------------------------------------------------------------------
// Custom text codec test
// ---------------------------------------------------------------------------

primitive \nodoc\ _TestUppercaseTextCodec is Codec
  """
  Trivial custom text codec for testing the text branch of with_codec.
  Decodes by uppercasing the input.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    error

  fun decode(data: Array[U8] val): FieldData =>
    let s = String.from_array(data)
    recover val s.upper() end

primitive \nodoc\ _TestFailingTextCodec is Codec
  """
  Text codec that always errors on decode. Used to test that decode errors
  from registered text codecs propagate through `CodecRegistry.decode()`.
  """
  fun format(): U16 => 0

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    error

  fun decode(data: Array[U8] val): FieldData ? =>
    error

class \nodoc\ iso _TestCodecRegistryWithCodecCustomText is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCodec/CustomTextCodec"

  fun apply(h: TestHelper) ? =>
    let reg = CodecRegistry.with_codec(99900, _TestUppercaseTextCodec)
    // Custom text codec should decode via the text codec map
    let data: Array[U8] val = "hello".array()
    match reg.decode(99900, 0, data)?
    | let s: String => h.assert_eq[String]("HELLO", s)
    else h.fail("Expected String from custom text codec")
    end
    // Binary format for same OID should fall back to RawBytes
    // (no binary codec registered)
    match reg.decode(99900, 1, data)?
    | let r: RawBytes => h.assert_array_eq[U8](data, r.data)
    else h.fail("Expected RawBytes fallback for binary format of text-only codec")
    end

class \nodoc\ iso _TestCodecRegistryDecodeErrorPropagatesText is UnitTest
  """
  A registered text codec whose `decode()` errors propagates the error
  through `CodecRegistry.decode()` instead of falling back to `String`.
  """
  fun name(): String =>
    "CodecRegistry/Decode/ErrorPropagates/Text"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry.with_codec(99901, _TestFailingTextCodec)
    let data: Array[U8] val = "hello".array()
    h.assert_error({()? => reg.decode(99901, 0, data)? })

class \nodoc\ iso _TestCodecRegistryDecodeErrorPropagatesBinary is UnitTest
  """
  A registered binary codec whose `decode()` errors propagates the error
  through `CodecRegistry.decode()` instead of falling back to `RawBytes`.
  """
  fun name(): String =>
    "CodecRegistry/Decode/ErrorPropagates/Binary"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry.with_codec(600, _TestPointCodec)
    // _TestPointCodec expects 16 bytes; 1 byte triggers decode error
    let data: Array[U8] val = recover val [1] end
    h.assert_error({()? => reg.decode(600, 1, data)? })

class \nodoc\ iso _TestCodecRegistryDecodeErrorPropagatesBuiltin is UnitTest
  """
  A built-in codec whose `decode()` errors (malformed server data)
  propagates through `CodecRegistry.decode()`.
  """
  fun name(): String =>
    "CodecRegistry/Decode/ErrorPropagates/Builtin"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    // OID 23 (int4) expects 4 bytes; 2 bytes triggers decode error
    let data: Array[U8] val = recover val [0; 0] end
    h.assert_error({()? => reg.decode(23, 1, data)? })
