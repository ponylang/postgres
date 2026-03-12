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
    h.assert_is[FieldDataTypes](true, codec.decode(encoded_true)?)

    let encoded_false = codec.encode(false)?
    h.assert_eq[USize](1, encoded_false.size())
    h.assert_eq[U8](0, encoded_false(0)?)
    h.assert_is[FieldDataTypes](false, codec.decode(encoded_false)?)

class \nodoc\ iso _TestBoolBinaryCodecNonzeroTrue is UnitTest
  fun name(): String =>
    "Codec/Binary/Bool/NonzeroTrue"

  fun apply(h: TestHelper) ? =>
    // Any nonzero byte decodes as true
    let data: Array[U8] val = recover val [42] end
    h.assert_is[FieldDataTypes](true, _BoolBinaryCodec.decode(data)?)

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
    | let decoded: Array[U8] val => h.assert_array_eq[U8](data, decoded)
    else h.fail("Expected Array[U8] val from decode")
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
    h.assert_is[FieldDataTypes](true, codec.decode(encoded_true))

    let encoded_false = codec.encode(false)?
    h.assert_array_eq[U8]("f".array(), encoded_false)
    h.assert_is[FieldDataTypes](false, codec.decode(encoded_false))

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
    | let decoded: Array[U8] val => h.assert_array_eq[U8](data, decoded)
    else h.fail("Expected Array[U8] val from decode")
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

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    // Unknown OID in text format should fall back to String
    let data: Array[U8] val = "hello".array()
    match reg.decode(99999, 0, data)
    | let s: String => h.assert_eq[String]("hello", s)
    else h.fail("Expected String fallback for unknown text OID")
    end

class \nodoc\ iso _TestCodecRegistryDecodeUnknownBinary is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/UnknownBinary"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    // Unknown OID in binary format should fall back to raw bytes
    let data: Array[U8] val = recover val [1; 2; 3] end
    match reg.decode(99999, 1, data)
    | let a: Array[U8] val => h.assert_array_eq[U8](data, a)
    else h.fail("Expected Array[U8] fallback for unknown binary OID")
    end

class \nodoc\ iso _TestCodecRegistryDecodeKnown is UnitTest
  fun name(): String =>
    "CodecRegistry/Decode/Known"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    // OID 16 (bool) text format, "t" -> true
    let data: Array[U8] val = "t".array()
    h.assert_is[FieldDataTypes](true, reg.decode(16, 0, data))

class \nodoc\ iso _TestCodecRegistryHasBinaryCodec is UnitTest
  fun name(): String =>
    "CodecRegistry/HasBinaryCodec"

  fun apply(h: TestHelper) =>
    let reg = CodecRegistry
    h.assert_true(reg.has_binary_codec(16))   // bool
    h.assert_true(reg.has_binary_codec(23))   // int4
    h.assert_true(reg.has_binary_codec(701))  // float8
    h.assert_false(reg.has_binary_codec(25))  // text — no binary codec
    h.assert_false(reg.has_binary_codec(99999))

class \nodoc\ iso _TestParamEncoderOids is UnitTest
  fun name(): String =>
    "ParamEncoder/Oids"

  fun apply(h: TestHelper) =>
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: I16(1); I32(2); I64(3); F32(1.0); F64(2.0)
        true; recover val [as U8: 0] end; "text"; None]
    end
    let oids = _ParamEncoder.oids_for(params)
    h.assert_eq[USize](9, oids.size())
    try
      h.assert_eq[U32](21, oids(0)?)   // I16
      h.assert_eq[U32](23, oids(1)?)   // I32
      h.assert_eq[U32](20, oids(2)?)   // I64
      h.assert_eq[U32](700, oids(3)?)  // F32
      h.assert_eq[U32](701, oids(4)?)  // F64
      h.assert_eq[U32](16, oids(5)?)   // Bool
      h.assert_eq[U32](17, oids(6)?)   // Array[U8]
      h.assert_eq[U32](0, oids(7)?)    // String
      h.assert_eq[U32](0, oids(8)?)    // None
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
    // Length = 4 + 0+1 + 0+1 + 2 + 1*2 + 2 + 8 + 2 = 22, total = 23
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: I32(42)]
    end
    let result = _FrontendMessage.bind("", "", params)?
    h.assert_eq[USize](23, result.size())

    // Check format code is binary (1)
    ifdef bigendian then
      // format code count
      h.assert_eq[U8](1, result(7)?)
      h.assert_eq[U8](0, result(8)?)
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
    // Length = 4 + 0+1 + 0+1 + 2 + 3*2 + 2 + 21 + 2 = 39, total = 40
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: "hello"; I32(42); None]
    end
    let result = _FrontendMessage.bind("", "", params)?
    h.assert_eq[USize](40, result.size())

class \nodoc\ iso _TestFrontendMessageBindEmptyParams is UnitTest
  fun name(): String =>
    "FrontendMessage/BindEmptyParams"

  fun apply(h: TestHelper) ? =>
    // Bind("", "", [])
    // No format codes, no params
    // Length = 4 + 0+1 + 0+1 + 2 + 0 + 2 + 0 + 2 = 12, total = 13
    let params: Array[FieldDataTypes] val = recover val Array[FieldDataTypes] end
    let result = _FrontendMessage.bind("", "", params)?
    h.assert_eq[USize](13, result.size())
