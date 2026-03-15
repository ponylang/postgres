use "cli"
use "collections"
use "constrained_types"
use lori = "lori"
use "pony_check"
use "pony_test"

// ============================================================
// _ArrayOidMap tests
// ============================================================

class \nodoc\ iso _TestArrayOidMapElementOidFor is UnitTest
  fun name(): String =>
    "ArrayOidMap/ElementOidFor"

  fun apply(h: TestHelper) ? =>
    h.assert_eq[U32](23, _ArrayOidMap.element_oid_for(1007)?)   // int4[]
    h.assert_eq[U32](25, _ArrayOidMap.element_oid_for(1009)?)   // text[]
    h.assert_eq[U32](16, _ArrayOidMap.element_oid_for(1000)?)   // bool[]
    h.assert_eq[U32](701, _ArrayOidMap.element_oid_for(1022)?)  // float8[]
    h.assert_eq[U32](2950, _ArrayOidMap.element_oid_for(2951)?) // uuid[]
    h.assert_eq[U32](3802, _ArrayOidMap.element_oid_for(3807)?) // jsonb[]
    h.assert_error({()? => _ArrayOidMap.element_oid_for(9999)? })

class \nodoc\ iso _TestArrayOidMapArrayOidFor is UnitTest
  fun name(): String =>
    "ArrayOidMap/ArrayOidFor"

  fun apply(h: TestHelper) ? =>
    h.assert_eq[U32](1007, _ArrayOidMap.array_oid_for(23)?)    // int4
    h.assert_eq[U32](1009, _ArrayOidMap.array_oid_for(25)?)    // text
    h.assert_eq[U32](1000, _ArrayOidMap.array_oid_for(16)?)    // bool
    h.assert_eq[U32](1022, _ArrayOidMap.array_oid_for(701)?)   // float8
    h.assert_eq[U32](2951, _ArrayOidMap.array_oid_for(2950)?)  // uuid
    h.assert_eq[U32](3807, _ArrayOidMap.array_oid_for(3802)?)  // jsonb
    h.assert_error({()? => _ArrayOidMap.array_oid_for(9999)? })

class \nodoc\ iso _TestArrayOidMapIsArrayOid is UnitTest
  fun name(): String =>
    "ArrayOidMap/IsArrayOid"

  fun apply(h: TestHelper) =>
    h.assert_true(_ArrayOidMap.is_array_oid(1007))  // int4[]
    h.assert_true(_ArrayOidMap.is_array_oid(1009))  // text[]
    h.assert_true(_ArrayOidMap.is_array_oid(3807))  // jsonb[]
    h.assert_false(_ArrayOidMap.is_array_oid(23))   // int4 (not array)
    h.assert_false(_ArrayOidMap.is_array_oid(9999)) // unknown

// ============================================================
// Binary decode tests
// ============================================================

primitive \nodoc\ _TestArrayBinaryBuilder
  """
  Builds binary array wire data for testing.
  """
  fun apply(element_oid: U32,
    elements: Array[(Array[U8] val | None)] val): Array[U8] val
  =>
    try
      var has_null: U32 = 0
      var data_size: USize = 20
      for e in elements.values() do
        data_size = data_size + 4
        match e
        | None => has_null = 1
        | let b: Array[U8] val => data_size = data_size + b.size()
        end
      end

      let ndim: U32 = if elements.size() == 0 then 0 else 1 end

      recover val
        let msg = Array[U8].init(0, data_size)
        ifdef bigendian then
          msg.update_u32(0, ndim)?
          msg.update_u32(4, has_null)?
          msg.update_u32(8, element_oid)?
        else
          msg.update_u32(0, ndim.bswap())?
          msg.update_u32(4, has_null.bswap())?
          msg.update_u32(8, element_oid.bswap())?
        end
        if ndim == 1 then
          ifdef bigendian then
            msg.update_u32(12, elements.size().u32())?
            msg.update_u32(16, U32(1))?  // lower_bound
          else
            msg.update_u32(12, elements.size().u32().bswap())?
            msg.update_u32(16, U32(1).bswap())?
          end
          var offset: USize = 20
          for e in elements.values() do
            match e
            | None =>
              ifdef bigendian then
                msg.update_u32(offset, U32.max_value())?
              else
                msg.update_u32(offset, U32.max_value().bswap())?
              end
              offset = offset + 4
            | let b: Array[U8] val =>
              ifdef bigendian then
                msg.update_u32(offset, b.size().u32())?
              else
                msg.update_u32(offset, b.size().u32().bswap())?
              end
              offset = offset + 4
              msg.copy_from(b, 0, offset, b.size())
              offset = offset + b.size()
            end
          end
        end
        msg
      end
    else
      _Unreachable()
      recover val Array[U8] end
    end

  fun int4_bytes(v: I32): Array[U8] val =>
    try
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
      _Unreachable()
      recover val Array[U8] end
    end

  fun int2_bytes(v: I16): Array[U8] val =>
    try
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
      _Unreachable()
      recover val Array[U8] end
    end

  fun bool_bytes(v: Bool): Array[U8] val =>
    recover val [if v then U8(1) else U8(0) end] end

class \nodoc\ iso _TestBinaryDecodeInt4Array is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/Int4/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let elems: Array[(Array[U8] val | None)] val = recover val
      [as (Array[U8] val | None):
        _TestArrayBinaryBuilder.int4_bytes(1)
        _TestArrayBinaryBuilder.int4_bytes(2)
        _TestArrayBinaryBuilder.int4_bytes(3)]
    end
    let data = _TestArrayBinaryBuilder(23, elems)
    let registry = CodecRegistry
    let result = registry.decode(1007, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      h.assert_eq[U32](23, arr.element_oid)
      try
        match arr(0)?
        | let v: I32 => h.assert_eq[I32](1, v)
        else h.fail("Expected I32")
        end
        match arr(1)?
        | let v: I32 => h.assert_eq[I32](2, v)
        else h.fail("Expected I32")
        end
        match arr(2)?
        | let v: I32 => h.assert_eq[I32](3, v)
        else h.fail("Expected I32")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeInt2Array is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/Int2/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let elems: Array[(Array[U8] val | None)] val = recover val
      [as (Array[U8] val | None):
        _TestArrayBinaryBuilder.int2_bytes(10)
        _TestArrayBinaryBuilder.int2_bytes(-5)]
    end
    let data = _TestArrayBinaryBuilder(21, elems)
    let result = CodecRegistry.decode(1005, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: I16 => h.assert_eq[I16](10, v)
        else h.fail("Expected I16")
        end
        match arr(1)?
        | let v: I16 => h.assert_eq[I16](-5, v)
        else h.fail("Expected I16")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeBoolArray is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/Bool/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let elems: Array[(Array[U8] val | None)] val = recover val
      [as (Array[U8] val | None):
        _TestArrayBinaryBuilder.bool_bytes(true)
        _TestArrayBinaryBuilder.bool_bytes(false)]
    end
    let data = _TestArrayBinaryBuilder(16, elems)
    let result = CodecRegistry.decode(1000, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: Bool => h.assert_true(v)
        else h.fail("Expected Bool")
        end
        match arr(1)?
        | let v: Bool => h.assert_false(v)
        else h.fail("Expected Bool")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeTextArray is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/Text/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let elems: Array[(Array[U8] val | None)] val = recover val
      [as (Array[U8] val | None):
        recover val "hello".array() end
        recover val "world".array() end]
    end
    let data = _TestArrayBinaryBuilder(25, elems)
    let result = CodecRegistry.decode(1009, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: String => h.assert_eq[String]("hello", v)
        else h.fail("Expected String")
        end
        match arr(1)?
        | let v: String => h.assert_eq[String]("world", v)
        else h.fail("Expected String")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeWithNulls is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/NullElements"

  fun apply(h: TestHelper) ? =>
    let elems: Array[(Array[U8] val | None)] val = recover val
      [as (Array[U8] val | None):
        _TestArrayBinaryBuilder.int4_bytes(1)
        None
        _TestArrayBinaryBuilder.int4_bytes(3)]
    end
    let data = _TestArrayBinaryBuilder(23, elems)
    let result = CodecRegistry.decode(1007, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      try
        match arr(0)?
        | let v: I32 => h.assert_eq[I32](1, v)
        else h.fail("Expected I32")
        end
        match arr(1)?
        | None => None
        else h.fail("Expected None")
        end
        match arr(2)?
        | let v: I32 => h.assert_eq[I32](3, v)
        else h.fail("Expected I32")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeEmptyArray is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/Empty"

  fun apply(h: TestHelper) ? =>
    // ndim=0 empty array
    let data = _TestArrayBinaryBuilder(23,
      recover val Array[(Array[U8] val | None)] end)
    let result = CodecRegistry.decode(1007, 1, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](0, arr.size())
      h.assert_eq[U32](23, arr.element_oid)
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestBinaryDecodeValidationErrors is UnitTest
  fun name(): String =>
    "Codec/Binary/Array/ValidationErrors"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry
    // ndim > 1 (multi-dimensional)
    let multidim = try
      recover val
        let a = Array[U8].init(0, 12)
        ifdef bigendian then
          a.update_u32(0, U32(2))?  // ndim=2
        else
          a.update_u32(0, U32(2).bswap())?
        end
        a
      end
    else
      _Unreachable()
      recover val Array[U8] end
    end
    match registry.decode(1007, 1, multidim)?
    | let _: RawBytes => None  // Falls back to RawBytes on error
    else
      h.fail("Expected fallback for multi-dimensional array")
    end

    // Truncated data (< 12 bytes)
    match registry.decode(1007, 1, recover val [as U8: 0; 0; 0] end)?
    | let _: RawBytes => None
    else
      h.fail("Expected fallback for truncated data")
    end

// ============================================================
// Text decode tests
// ============================================================

class \nodoc\ iso _TestTextDecodeSimpleArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Simple"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{1,2,3}".array() end
    let result = CodecRegistry.decode(1007, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      h.assert_eq[U32](23, arr.element_oid)
      try
        match arr(0)?
        | let v: I32 => h.assert_eq[I32](1, v)
        else h.fail("Expected I32")
        end
        match arr(2)?
        | let v: I32 => h.assert_eq[I32](3, v)
        else h.fail("Expected I32")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeNullArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Null"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{1,NULL,3}".array() end
    let result = CodecRegistry.decode(1007, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      try
        match arr(1)?
        | None => None
        else h.fail("Expected None for NULL")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeQuotedArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Quoted"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val =
      recover val "{\"hello, world\",\"simple\"}".array() end
    let result = CodecRegistry.decode(1009, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: String => h.assert_eq[String]("hello, world", v)
        else h.fail("Expected String")
        end
        match arr(1)?
        | let v: String => h.assert_eq[String]("simple", v)
        else h.fail("Expected String")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeEscapedArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Escaped"

  fun apply(h: TestHelper) ? =>
    // Element with escaped quotes: {"with \"quotes\""}
    let data: Array[U8] val =
      recover val "{\"with \\\"quotes\\\"\"}".array() end
    let result = CodecRegistry.decode(1009, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](1, arr.size())
      try
        match arr(0)?
        | let v: String =>
          h.assert_eq[String]("with \"quotes\"", v)
        else h.fail("Expected String")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeEmptyStringArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/EmptyString"

  fun apply(h: TestHelper) ? =>
    // Empty string element: {""}
    let data: Array[U8] val = recover val "{\"\"}".array() end
    let result = CodecRegistry.decode(1009, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](1, arr.size())
      try
        match arr(0)?
        | let v: String => h.assert_eq[String]("", v)
        else h.fail("Expected empty String")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeEmptyArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Empty"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{}".array() end
    let result = CodecRegistry.decode(1007, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](0, arr.size())
      h.assert_eq[U32](23, arr.element_oid)
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeMultiDimensionalRejected is UnitTest
  fun name(): String =>
    "Codec/Text/Array/MultiDimensionalRejected"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{{1,2},{3,4}}".array() end
    // Multi-dimensional arrays are rejected — falls back to String
    let result = CodecRegistry.decode(1007, 0, data)?
    match result
    | let _: String => None
    else
      h.fail("Expected String fallback for multi-dimensional")
    end

class \nodoc\ iso _TestTextDecodeBoolArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Bool"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{t,f,t}".array() end
    let result = CodecRegistry.decode(1000, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      try
        match arr(0)?
        | let v: Bool => h.assert_true(v)
        else h.fail("Expected Bool")
        end
        match arr(1)?
        | let v: Bool => h.assert_false(v)
        else h.fail("Expected Bool")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeCaseInsensitiveNull is UnitTest
  fun name(): String =>
    "Codec/Text/Array/CaseInsensitiveNull"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{1,null,Null,NULL}".array() end
    let result = CodecRegistry.decode(1007, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](4, arr.size())
      try
        match arr(1)?
        | None => None
        else h.fail("Expected None for 'null'")
        end
        match arr(2)?
        | None => None
        else h.fail("Expected None for 'Null'")
        end
        match arr(3)?
        | None => None
        else h.fail("Expected None for 'NULL'")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

// ============================================================
// PgArray equality tests
// ============================================================

class \nodoc\ iso _TestPgArrayEquality is UnitTest
  fun name(): String =>
    "PgArray/Equality"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let b = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    h.assert_true(a == b)

class \nodoc\ iso _TestPgArrayInequalityDifferentOid is UnitTest
  fun name(): String =>
    "PgArray/Inequality/DifferentOid"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1)] end)
    let b = PgArray(21,
      recover val [as (FieldData | None): I32(1)] end)
    h.assert_false(a == b)

class \nodoc\ iso _TestPgArrayInequalityDifferentElements is UnitTest
  fun name(): String =>
    "PgArray/Inequality/DifferentElements"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let b = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(3)] end)
    h.assert_false(a == b)

class \nodoc\ iso _TestPgArrayEqualityWithNulls is UnitTest
  fun name(): String =>
    "PgArray/Equality/WithNulls"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1); None; I32(3)] end)
    let b = PgArray(23,
      recover val [as (FieldData | None): I32(1); None; I32(3)] end)
    h.assert_true(a == b)

class \nodoc\ iso _TestPgArrayString is UnitTest
  fun name(): String =>
    "PgArray/String"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1); None; I32(3)] end)
    h.assert_eq[String]("{1,NULL,3}", a.string())

    let empty = PgArray(23,
      recover val Array[(FieldData | None)] end)
    h.assert_eq[String]("{}", empty.string())

class \nodoc\ iso _TestPgArrayStringQuoting is UnitTest
  fun name(): String =>
    "PgArray/String/Quoting"

  fun apply(h: TestHelper) =>
    // Element with comma
    let a = PgArray(25,
      recover val [as (FieldData | None): "hello, world"] end)
    h.assert_eq[String]("{\"hello, world\"}", a.string())

    // Element with quotes
    let b = PgArray(25,
      recover val [as (FieldData | None): "with \"quotes\""] end)
    h.assert_eq[String]("{\"with \\\"quotes\\\"\"}", b.string())

    // Empty string element
    let c = PgArray(25,
      recover val [as (FieldData | None): ""] end)
    h.assert_eq[String]("{\"\"}", c.string())

// ============================================================
// _ArrayEncoder roundtrip tests
// ============================================================

class \nodoc\ iso _TestArrayEncoderRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let original = PgArray(23,
      recover val [as (FieldData | None): I32(10); None; I32(30)] end)
    let encoded = _ArrayEncoder(original)?
    let decoded = CodecRegistry.decode(1007, 1, encoded)?
    match decoded
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      h.assert_eq[U32](23, arr.element_oid)
      try
        match arr(0)?
        | let v: I32 => h.assert_eq[I32](10, v)
        else h.fail("Expected I32")
        end
        match arr(1)?
        | None => None
        else h.fail("Expected None")
        end
        match arr(2)?
        | let v: I32 => h.assert_eq[I32](30, v)
        else h.fail("Expected I32")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderEmptyRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Empty/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let original = PgArray(23,
      recover val Array[(FieldData | None)] end)
    let encoded = _ArrayEncoder(original)?
    let decoded = CodecRegistry.decode(1007, 1, encoded)?
    match decoded
    | let arr: PgArray =>
      h.assert_eq[USize](0, arr.size())
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderBoolRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Bool/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let original = PgArray(16,
      recover val [as (FieldData | None): true; false; true] end)
    let encoded = _ArrayEncoder(original)?
    let decoded = CodecRegistry.decode(1000, 1, encoded)?
    match decoded
    | let arr: PgArray =>
      h.assert_eq[USize](3, arr.size())
      try
        match arr(0)?
        | let v: Bool => h.assert_true(v)
        else h.fail("Expected Bool")
        end
        match arr(1)?
        | let v: Bool => h.assert_false(v)
        else h.fail("Expected Bool")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderStringRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/String/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let original = PgArray(25,
      recover val [as (FieldData | None): "hello"; "world"] end)
    let encoded = _ArrayEncoder(original)?
    let decoded = CodecRegistry.decode(1009, 1, encoded)?
    match decoded
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: String => h.assert_eq[String]("hello", v)
        else h.fail("Expected String")
        end
      else
        h.fail("Array access error")
      end
    else
      h.fail("Expected PgArray")
    end

// ============================================================
// _ParamEncoder with PgArray
// ============================================================

class \nodoc\ iso _TestParamEncoderPgArrayOids is UnitTest
  fun name(): String =>
    "ParamEncoder/PgArray/Oids"

  fun apply(h: TestHelper) =>
    let arr = PgArray(23,
      recover val [as (FieldData | None): I32(1)] end)
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: arr]
    end
    let oids = _ParamEncoder.oids_for(params, CodecRegistry)
    h.assert_eq[USize](1, oids.size())
    try
      h.assert_eq[U32](1007, oids(0)?)  // int4[] OID
    else
      h.fail("Error accessing OIDs")
    end

class \nodoc\ iso _TestParamEncoderPgArrayUnknownOid is UnitTest
  fun name(): String =>
    "ParamEncoder/PgArray/UnknownOid"

  fun apply(h: TestHelper) =>
    let arr = PgArray(9999,
      recover val [as (FieldData | None): I32(1)] end)
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: arr]
    end
    let oids = _ParamEncoder.oids_for(params, CodecRegistry)
    try
      h.assert_eq[U32](0, oids(0)?)  // unknown → server infers
    else
      h.fail("Error accessing OIDs")
    end

// ============================================================
// _FrontendMessage.bind() with PgArray
// ============================================================

class \nodoc\ iso _TestFrontendMessageBindWithPgArray is UnitTest
  fun name(): String =>
    "FrontendMessage/Bind/PgArray"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: arr]
    end
    let result = _FrontendMessage.bind("", "", params, CodecRegistry)?
    // Just verify it doesn't error and produces a non-empty result
    h.assert_true(result.size() > 15)
    // Format code should be binary (1)
    // Format codes start at offset 9 (after B + 4-byte length + 2 nulls
    // + 2-byte num_param_formats)
    h.assert_eq[U8](0, result(9)?)
    h.assert_eq[U8](1, result(10)?)

// ============================================================
// _FieldDataEq extraction test
// ============================================================

class \nodoc\ iso _TestFieldDataEqExtraction is UnitTest
  fun name(): String =>
    "FieldDataEq/Extraction"

  fun apply(h: TestHelper) =>
    // Verify Field.eq() still works after refactoring to _FieldDataEq
    let f1 = Field("x", I32(42))
    let f2 = Field("x", I32(42))
    let f3 = Field("x", I32(99))
    let f4 = Field("y", I32(42))
    h.assert_true(f1 == f2)
    h.assert_false(f1 == f3)
    h.assert_false(f1 == f4)

    // Field equality with PgArray
    let arr1 = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let arr2 = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let fa = Field("arr", arr1)
    let fb = Field("arr", arr2)
    h.assert_true(fa == fb)

// ============================================================
// _NumericBinaryCodec encode roundtrip tests
// ============================================================

class \nodoc\ iso _TestNumericBinaryCodecEncodeRoundtrip is UnitTest
  fun name(): String =>
    "Codec/Binary/Numeric/EncodeRoundtrip"

  fun apply(h: TestHelper) ? =>
    _check(h, "42")?
    _check(h, "-42")?
    _check(h, "0")?
    _check(h, "123456789")?
    _check(h, "3.14")?
    _check(h, "-99.99")?
    _check(h, "0.001")?
    _check(h, "0.0001")?
    _check(h, "1000000.00")?
    _check(h, "9999")?
    _check(h, "10000")?
    _check(h, "NaN")?
    _check(h, "Infinity")?
    _check(h, "-Infinity")?

  fun _check(h: TestHelper, value: String) ? =>
    let encoded = _NumericBinaryCodec.encode(value)?
    let decoded = _NumericBinaryCodec.decode(encoded)?
    match decoded
    | let s: String =>
      h.assert_eq[String](value, s)
    else
      h.fail("Expected String for numeric " + value)
    end

// ============================================================
// CodecRegistry array-related tests
// ============================================================

class \nodoc\ iso _TestCodecRegistryHasBinaryCodecArray is UnitTest
  fun name(): String =>
    "CodecRegistry/HasBinaryCodec/Array"

  fun apply(h: TestHelper) =>
    let registry = CodecRegistry
    h.assert_true(registry.has_binary_codec(1007))  // int4[]
    h.assert_true(registry.has_binary_codec(1009))  // text[]
    h.assert_true(registry.has_binary_codec(23))    // int4 (scalar)
    h.assert_false(registry.has_binary_codec(9999)) // unknown

class \nodoc\ iso _TestCodecRegistryWithArrayType is UnitTest
  fun name(): String =>
    "CodecRegistry/WithArrayType"

  fun apply(h: TestHelper) =>
    let registry = CodecRegistry.with_array_type(1017, 600)
    h.assert_true(registry.has_binary_codec(1017))
    h.assert_eq[U32](1017, registry.array_oid_for(600))

class \nodoc\ iso _TestCodecRegistryArrayOidFor is UnitTest
  fun name(): String =>
    "CodecRegistry/ArrayOidFor"

  fun apply(h: TestHelper) =>
    let registry = CodecRegistry
    h.assert_eq[U32](1007, registry.array_oid_for(23))   // int4
    h.assert_eq[U32](0, registry.array_oid_for(9999))    // unknown

// ============================================================
// Integration tests (require PostgreSQL)
// ============================================================

class \nodoc\ iso _TestIntegrationArraySelectBinary is UnitTest
  """
  SELECT an int4[] via PreparedQuery (binary decode).
  """
  fun name(): String =>
    "integration/Array/SelectBinary"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayTestClient(h,
      "SELECT ARRAY[1,2,3]::int4[] AS arr", true)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

class \nodoc\ iso _TestIntegrationArraySelectText is UnitTest
  """
  SELECT an int4[] via SimpleQuery (text decode).
  """
  fun name(): String =>
    "integration/Array/SelectText"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayTestClient(h,
      "SELECT ARRAY[1,2,3]::int4[] AS arr", false)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

class \nodoc\ iso _TestIntegrationArrayRoundtrip is UnitTest
  """
  Send a PgArray as parameter, SELECT it back.
  """
  fun name(): String =>
    "integration/Array/Roundtrip"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayRoundtripClient(h)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

class \nodoc\ iso _TestIntegrationArrayEmpty is UnitTest
  """
  SELECT an empty array.
  """
  fun name(): String =>
    "integration/Array/Empty"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayEmptyClient(h)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

class \nodoc\ iso _TestIntegrationArrayNulls is UnitTest
  """
  SELECT an array with NULLs.
  """
  fun name(): String =>
    "integration/Array/Nulls"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayNullsClient(h)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

class \nodoc\ iso _TestIntegrationArrayMultipleTypes is UnitTest
  """
  SELECT arrays of multiple element types.
  """
  fun name(): String =>
    "integration/Array/MultipleTypes"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _ArrayMultiTypeClient(h)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(10_000_000_000)

// ============================================================
// Integration test helper actors
// ============================================================

actor \nodoc\ _ArrayTestClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper
  let _query_str: String
  let _use_prepared: Bool

  new create(h: TestHelper, query_str: String, use_prepared: Bool) =>
    _h = h
    _query_str = query_str
    _use_prepared = use_prepared

  be pg_session_authenticated(session: Session) =>
    if _use_prepared then
      session.execute(PreparedQuery(_query_str,
        recover val Array[FieldDataTypes] end), this)
    else
      session.execute(SimpleQuery(_query_str), this)
    end

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let rs: ResultSet =>
      try
        let row = rs.rows()(0)?
        let field = row.fields(0)?
        match field.value
        | let arr: PgArray =>
          _h.assert_eq[USize](3, arr.size())
          try
            match arr(0)?
            | let v: I32 => _h.assert_eq[I32](1, v)
            else _h.fail("Expected I32")
            end
          else
            _h.fail("Array access error")
          end
          _h.complete(true)
        else
          _h.fail("Expected PgArray, got something else")
          _h.complete(false)
        end
      else
        _h.fail("Row access error")
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet")
      _h.complete(false)
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed")
    _h.complete(false)
    session.close()


actor \nodoc\ _ArrayRoundtripClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    let arr = PgArray(23,
      recover val [as (FieldData | None): I32(10); I32(20); I32(30)] end)
    session.execute(PreparedQuery("SELECT $1::int4[] AS arr",
      recover val [as FieldDataTypes: arr] end), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let rs: ResultSet =>
      try
        let field = rs.rows()(0)?.fields(0)?
        match field.value
        | let arr: PgArray =>
          _h.assert_eq[USize](3, arr.size())
          try
            match arr(0)?
            | let v: I32 => _h.assert_eq[I32](10, v)
            else _h.fail("Expected I32")
            end
            match arr(2)?
            | let v: I32 => _h.assert_eq[I32](30, v)
            else _h.fail("Expected I32")
            end
          else
            _h.fail("Array access error")
          end
          _h.complete(true)
        else
          _h.fail("Expected PgArray")
          _h.complete(false)
        end
      else
        _h.fail("Row access error")
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet")
      _h.complete(false)
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed")
    _h.complete(false)
    session.close()


actor \nodoc\ _ArrayEmptyClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(PreparedQuery("SELECT '{}'::int4[] AS arr",
      recover val Array[FieldDataTypes] end), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let rs: ResultSet =>
      try
        let field = rs.rows()(0)?.fields(0)?
        match field.value
        | let arr: PgArray =>
          _h.assert_eq[USize](0, arr.size())
          _h.complete(true)
        else
          _h.fail("Expected PgArray")
          _h.complete(false)
        end
      else
        _h.fail("Row access error")
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet")
      _h.complete(false)
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed")
    _h.complete(false)
    session.close()


actor \nodoc\ _ArrayNullsClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    session.execute(PreparedQuery(
      "SELECT ARRAY[1,NULL,3]::int4[] AS arr",
      recover val Array[FieldDataTypes] end), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let rs: ResultSet =>
      try
        let field = rs.rows()(0)?.fields(0)?
        match field.value
        | let arr: PgArray =>
          _h.assert_eq[USize](3, arr.size())
          try
            match arr(1)?
            | None => None
            else _h.fail("Expected None for NULL element")
            end
          else
            _h.fail("Array access error")
          end
          _h.complete(true)
        else
          _h.fail("Expected PgArray")
          _h.complete(false)
        end
      else
        _h.fail("Row access error")
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet")
      _h.complete(false)
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed")
    _h.complete(false)
    session.close()


actor \nodoc\ _ArrayMultiTypeClient is (SessionStatusNotify & ResultReceiver)
  let _h: TestHelper

  new create(h: TestHelper) =>
    _h = h

  be pg_session_authenticated(session: Session) =>
    // Test multiple array types in sequence
    session.execute(PreparedQuery(
      "SELECT ARRAY[true,false]::bool[] AS b, ARRAY[1.5,2.5]::float8[] AS f",
      recover val Array[FieldDataTypes] end), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)

  be pg_query_result(session: Session, result: Result) =>
    match result
    | let rs: ResultSet =>
      try
        let row = rs.rows()(0)?
        // bool[]
        match row.fields(0)?.value
        | let arr: PgArray =>
          _h.assert_eq[USize](2, arr.size())
        else
          _h.fail("Expected PgArray for bool[]")
        end
        // float8[]
        match row.fields(1)?.value
        | let arr: PgArray =>
          _h.assert_eq[USize](2, arr.size())
        else
          _h.fail("Expected PgArray for float8[]")
        end
        // Verify actual element values for bool[]
        match row.fields(0)?.value
        | let ba: PgArray =>
          try
            match ba(0)?
            | let v: Bool => _h.assert_true(v)
            else _h.fail("Expected Bool in bool[]")
            end
            match ba(1)?
            | let v: Bool => _h.assert_false(v)
            else _h.fail("Expected Bool in bool[]")
            end
          else _h.fail("Bool array access error")
          end
        end
        // Verify actual element values for float8[]
        match row.fields(1)?.value
        | let fa: PgArray =>
          try
            match fa(0)?
            | let v: F64 => _h.assert_eq[F64](1.5, v)
            else _h.fail("Expected F64 in float8[]")
            end
            match fa(1)?
            | let v: F64 => _h.assert_eq[F64](2.5, v)
            else _h.fail("Expected F64 in float8[]")
            end
          else _h.fail("Float8 array access error")
          end
        end
        _h.complete(true)
      else
        _h.fail("Row access error")
        _h.complete(false)
      end
    else
      _h.fail("Expected ResultSet")
      _h.complete(false)
    end
    session.close()

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    _h.fail("Query failed")
    _h.complete(false)
    session.close()

// ============================================================
// Generator for property-based tests
// ============================================================

primitive \nodoc\ _PgArrayGen
  """
  Generator for (PgArray, array_oid) pairs across all supported element
  types. Used by property-based roundtrip and equality tests.
  """
  fun apply(): Generator[(PgArray, U32)] =>
    Generator[(PgArray, U32)](object is GenObj[(PgArray, U32)]
      fun generate(rnd: Randomness): (PgArray, U32) =>
        match rnd.usize(0, 17)
        | 0 => _gen(rnd, 16, 1000)      // bool
        | 1 => _gen(rnd, 21, 1005)      // int2
        | 2 => _gen(rnd, 23, 1007)      // int4
        | 3 => _gen(rnd, 20, 1016)      // int8
        | 4 => _gen(rnd, 700, 1021)     // float4
        | 5 => _gen(rnd, 701, 1022)     // float8
        | 6 => _gen(rnd, 25, 1009)      // text
        | 7 => _gen(rnd, 1043, 1015)    // varchar
        | 8 => _gen(rnd, 17, 1001)      // bytea
        | 9 => _gen(rnd, 1082, 1182)    // date
        | 10 => _gen(rnd, 1083, 1183)   // time
        | 11 => _gen(rnd, 1114, 1115)   // timestamp
        | 12 => _gen(rnd, 1184, 1185)   // timestamptz
        | 13 => _gen(rnd, 1186, 1187)   // interval
        | 14 => _gen(rnd, 2950, 2951)   // uuid
        | 15 => _gen(rnd, 1700, 1231)   // numeric
        | 16 => _gen(rnd, 26, 1028)     // oid
        else
          _gen(rnd, 3802, 3807)          // jsonb
        end

      fun _gen(rnd: Randomness, element_oid: U32, array_oid: U32)
        : (PgArray, U32)
      =>
        let size = rnd.usize(0, 5)
        let elems = recover iso Array[(FieldData | None)](size) end
        for _ in Range(0, size) do
          if rnd.usize(0, 4) == 0 then
            elems.push(None)
          else
            elems.push(_value(rnd, element_oid))
          end
        end
        (PgArray(element_oid, consume elems), array_oid)

      fun _value(rnd: Randomness, oid: U32): FieldData =>
        match oid
        | 16 => rnd.bool()
        | 21 => rnd.i16()
        | 23 => rnd.i32()
        | 20 => rnd.i64()
        | 700 => F32.from[I32](rnd.i32())
        | 701 => F64.from[I64](rnd.i64())
        | 17 =>
          Bytea(recover val
            let a = Array[U8](rnd.usize(0, 10))
            for _ in Range(0, a.space()) do
              a.push(rnd.u8())
            end
            a
          end)
        | 1082 => PgDate(rnd.i32())
        | 1083 =>
          try
            PgTime(MakePgTimeMicroseconds(
              (rnd.i64().abs() % 86_400_000_000).i64())
              as PgTimeMicroseconds)
          else _Unreachable(); I64(0)
          end
        | 1114 | 1184 => PgTimestamp(rnd.i64())
        | 1186 => PgInterval(rnd.i64(), rnd.i32(), rnd.i32())
        | 2950 => _uuid(rnd)
        | 1700 => _numeric(rnd)
        | 26 => rnd.u32().string()
        | 3802 =>
          match rnd.usize(0, 2)
          | 0 => "{}"
          | 1 => "\"test\""
          else
            rnd.i32().string()
          end
        else
          // text-like types: generate simple alpha strings
          let len = rnd.usize(1, 10)
          recover val
            let s = String(len)
            for _ in Range(0, len) do
              s.push((rnd.usize(0, 25) + 97).u8())
            end
            s
          end
        end

      fun _uuid(rnd: Randomness): String =>
        let hex = "0123456789abcdef"
        recover val
          let s = String(36)
          for i in Range(0, 36) do
            if (i == 8) or (i == 13) or (i == 18) or (i == 23) then
              s.push('-')
            else
              try s.push(hex(rnd.usize(0, 15))?)
              else s.push('0')
              end
            end
          end
          s
        end

      fun _numeric(rnd: Randomness): String =>
        match rnd.usize(0, 4)
        | 0 => "0"
        | 1 => rnd.u16().string()
        | 2 =>
          let v = (rnd.usize(1, 65535)).string()
          recover val "-".clone().>append(consume v) end
        | 3 => "NaN"
        else
          "Infinity"
        end
    end)

// ============================================================
// Property-based tests
// ============================================================

class \nodoc\ iso _TestArrayBinaryRoundtripProperty
  is Property1[(PgArray, U32)]
  """
  For any generated PgArray, encoding to binary wire format and decoding
  back produces an identical PgArray. Covers all 18 element type categories.
  """
  fun name(): String =>
    "Array/Binary/Roundtrip/Property"

  fun gen(): Generator[(PgArray, U32)] =>
    _PgArrayGen()

  fun ref property(arg1: (PgArray, U32), h: PropertyHelper) ? =>
    (let arr, let array_oid) = arg1
    let encoded = _ArrayEncoder(arr)?
    let decoded = CodecRegistry.decode(array_oid, 1, encoded)?
    match decoded
    | let result: PgArray =>
      h.assert_eq[USize](arr.size(), result.size())
      h.assert_eq[U32](arr.element_oid, result.element_oid)
      h.assert_true(arr == result,
        "Roundtrip mismatch for element_oid=" + arr.element_oid.string()
        + " size=" + arr.size().string())
    else
      h.fail("Expected PgArray for array_oid " + array_oid.string())
    end

class \nodoc\ iso _TestPgArrayEqualityReflexiveProperty
  is Property1[(PgArray, U32)]
  """
  PgArray equality is reflexive: every array equals itself.
  """
  fun name(): String =>
    "PgArray/Equality/Reflexive/Property"

  fun gen(): Generator[(PgArray, U32)] =>
    _PgArrayGen()

  fun ref property(arg1: (PgArray, U32), h: PropertyHelper) =>
    (let arr, _) = arg1
    h.assert_true(arr == arr)

// ============================================================
// Encoder roundtrip tests — remaining element types
// ============================================================

class \nodoc\ iso _TestArrayEncoderI16Roundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/I16/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(21,
      recover val [as (FieldData | None): I16(10); None; I16(-5)] end)
    let decoded = CodecRegistry.decode(1005, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray =>
      h.assert_eq[USize](3, r.size())
      h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderI64Roundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/I64/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(20,
      recover val [as (FieldData | None):
        I64(1000000); None; I64(-999)] end)
    let decoded = CodecRegistry.decode(1016, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderF32Roundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/F32/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(700,
      recover val [as (FieldData | None): F32(1.5); F32(-2.5)] end)
    let decoded = CodecRegistry.decode(1021, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderF64Roundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/F64/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(701,
      recover val [as (FieldData | None): F64(3.14); None; F64(-1.0)] end)
    let decoded = CodecRegistry.decode(1022, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderByteaRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Bytea/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(17,
      recover val [as (FieldData | None):
        Bytea(recover val [as U8: 1; 2; 3] end)
        None
        Bytea(recover val Array[U8] end)] end)
    let decoded = CodecRegistry.decode(1001, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderDateRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Date/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(1082,
      recover val [as (FieldData | None):
        PgDate(0); PgDate(365); PgDate(-365)] end)
    let decoded = CodecRegistry.decode(1182, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderTimeRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Time/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let t1 = PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds)
    let t2 = PgTime(
      MakePgTimeMicroseconds(43_200_000_000) as PgTimeMicroseconds)
    let arr = PgArray(1083,
      recover val [as (FieldData | None): t1; t2] end)
    let decoded = CodecRegistry.decode(1183, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderTimestampRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Timestamp/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(1114,
      recover val [as (FieldData | None):
        PgTimestamp(0); PgTimestamp(I64.max_value())
        PgTimestamp(I64.min_value())] end)
    let decoded = CodecRegistry.decode(1115, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderIntervalRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Interval/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(1186,
      recover val [as (FieldData | None):
        PgInterval(1_000_000, 30, 12)
        None
        PgInterval(0, 0, 0)] end)
    let decoded = CodecRegistry.decode(1187, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderUuidRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Uuid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(2950,
      recover val [as (FieldData | None):
        "550e8400-e29b-41d4-a716-446655440000"] end)
    let decoded = CodecRegistry.decode(2951, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray =>
      h.assert_eq[USize](1, r.size())
      try
        match r(0)?
        | let v: String =>
          h.assert_eq[String](
            "550e8400-e29b-41d4-a716-446655440000", v)
        else h.fail("Expected String")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderJsonbRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Jsonb/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(3802,
      recover val [as (FieldData | None):
        "{\"key\": \"value\"}"; "42"] end)
    let decoded = CodecRegistry.decode(3807, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderOidRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Oid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(26,
      recover val [as (FieldData | None): "12345"; "0"] end)
    let decoded = CodecRegistry.decode(1028, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestArrayEncoderNumericRoundtrip is UnitTest
  fun name(): String =>
    "ArrayEncoder/Numeric/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let arr = PgArray(1700,
      recover val [as (FieldData | None):
        "42"; "-99.99"; "0"; "NaN"] end)
    let decoded = CodecRegistry.decode(1231, 1, _ArrayEncoder(arr)?)?
    match decoded
    | let r: PgArray => h.assert_true(arr == r)
    else h.fail("Expected PgArray")
    end

// ============================================================
// Text decode — remaining element types
// ============================================================

class \nodoc\ iso _TestTextDecodeInt8Array is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Int8"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{100,-200}".array() end
    let result = CodecRegistry.decode(1016, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      h.assert_eq[U32](20, arr.element_oid)
      try
        match arr(0)?
        | let v: I64 => h.assert_eq[I64](100, v)
        else h.fail("Expected I64")
        end
        match arr(1)?
        | let v: I64 => h.assert_eq[I64](-200, v)
        else h.fail("Expected I64")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeFloat8Array is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Float8"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val "{1.5,-2.5}".array() end
    let result = CodecRegistry.decode(1022, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: F64 => h.assert_eq[F64](1.5, v)
        else h.fail("Expected F64")
        end
        match arr(1)?
        | let v: F64 => h.assert_eq[F64](-2.5, v)
        else h.fail("Expected F64")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeDateArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Date"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val =
      recover val "{2000-01-01,2001-01-01}".array() end
    let result = CodecRegistry.decode(1182, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      h.assert_eq[U32](1082, arr.element_oid)
      try
        match arr(0)?
        | let v: PgDate => h.assert_eq[I32](0, v.days)
        else h.fail("Expected PgDate")
        end
        match arr(1)?
        | let v: PgDate =>
          h.assert_eq[I32](366, v.days) // 2000 is leap year
        else h.fail("Expected PgDate")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeTimestampArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Timestamp"

  fun apply(h: TestHelper) ? =>
    // PostgreSQL quotes timestamps in arrays because they contain spaces
    let data: Array[U8] val =
      recover val
        "{\"2000-01-01 00:00:00\",\"2000-01-01 00:00:01\"}".array()
      end
    let result = CodecRegistry.decode(1115, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: PgTimestamp => h.assert_eq[I64](0, v.microseconds)
        else h.fail("Expected PgTimestamp")
        end
        match arr(1)?
        | let v: PgTimestamp =>
          h.assert_eq[I64](1_000_000, v.microseconds)
        else h.fail("Expected PgTimestamp")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeUuidArray is UnitTest
  fun name(): String =>
    "Codec/Text/Array/Uuid"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = recover val
      "{550e8400-e29b-41d4-a716-446655440000}".array()
    end
    let result = CodecRegistry.decode(2951, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](1, arr.size())
      try
        match arr(0)?
        | let v: String =>
          h.assert_eq[String](
            "550e8400-e29b-41d4-a716-446655440000", v)
        else h.fail("Expected String")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

// ============================================================
// Text decode — edge cases
// ============================================================

class \nodoc\ iso _TestTextDecodeEscapedBackslash is UnitTest
  fun name(): String =>
    "Codec/Text/Array/EscapedBackslash"

  fun apply(h: TestHelper) ? =>
    // Element with literal backslash: {"with \\backslash"}
    let data: Array[U8] val =
      recover val "{\"with \\\\backslash\"}".array() end
    let result = CodecRegistry.decode(1009, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](1, arr.size())
      try
        match arr(0)?
        | let v: String =>
          h.assert_eq[String]("with \\backslash", v)
        else h.fail("Expected String")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestTextDecodeQuotedNullString is UnitTest
  fun name(): String =>
    "Codec/Text/Array/QuotedNullString"

  fun apply(h: TestHelper) ? =>
    // Quoted "NULL" decodes as string, not SQL NULL
    let data: Array[U8] val =
      recover val "{\"NULL\",NULL}".array() end
    let result = CodecRegistry.decode(1009, 0, data)?
    match result
    | let arr: PgArray =>
      h.assert_eq[USize](2, arr.size())
      try
        match arr(0)?
        | let v: String => h.assert_eq[String]("NULL", v)
        else h.fail("Expected String 'NULL'")
        end
        match arr(1)?
        | None => None
        else h.fail("Expected None for unquoted NULL")
        end
      else h.fail("Array access error")
      end
    else h.fail("Expected PgArray")
    end

// ============================================================
// PgArray equality — edge cases
// ============================================================

class \nodoc\ iso _TestPgArrayEqualitySizeMismatch is UnitTest
  fun name(): String =>
    "PgArray/Equality/SizeMismatch"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val [as (FieldData | None): I32(1); I32(2)] end)
    let b = PgArray(23,
      recover val [as (FieldData | None): I32(1)] end)
    h.assert_false(a == b)

class \nodoc\ iso _TestPgArrayEqualityEmpty is UnitTest
  fun name(): String =>
    "PgArray/Equality/Empty"

  fun apply(h: TestHelper) =>
    let a = PgArray(23,
      recover val Array[(FieldData | None)] end)
    let b = PgArray(23,
      recover val Array[(FieldData | None)] end)
    h.assert_true(a == b)

class \nodoc\ iso _TestPgArrayFieldDataEqNonPgArray is UnitTest
  fun name(): String =>
    "PgArray/FieldDataEq/NonPgArray"

  fun apply(h: TestHelper) =>
    let arr = PgArray(23,
      recover val [as (FieldData | None): I32(1)] end)
    h.assert_false(arr.field_data_eq(I32(1)))
    h.assert_false(arr.field_data_eq("test"))

class \nodoc\ iso _TestPgArrayStringNull is UnitTest
  fun name(): String =>
    "PgArray/String/NullLookalike"

  fun apply(h: TestHelper) =>
    let a = PgArray(25,
      recover val [as (FieldData | None): "null"] end)
    h.assert_eq[String]("{\"null\"}", a.string())
    let b = PgArray(25,
      recover val [as (FieldData | None): "Null"] end)
    h.assert_eq[String]("{\"Null\"}", b.string())

// ============================================================
// _FieldDataEq.nullable — mismatch tests
// ============================================================

class \nodoc\ iso _TestFieldDataEqNullableMismatch is UnitTest
  fun name(): String =>
    "FieldDataEq/Nullable/Mismatch"

  fun apply(h: TestHelper) =>
    h.assert_true(_FieldDataEq.nullable(None, None))
    h.assert_false(_FieldDataEq.nullable(None, I32(1)))
    h.assert_false(_FieldDataEq.nullable(I32(1), None))
    h.assert_true(_FieldDataEq.nullable(I32(42), I32(42)))
    h.assert_false(_FieldDataEq.nullable(I32(1), I32(2)))

// ============================================================
// _ArrayEncoder — negative cases
// ============================================================

class \nodoc\ iso _TestArrayEncoderUnsupportedType is UnitTest
  fun name(): String =>
    "ArrayEncoder/UnsupportedType"

  fun apply(h: TestHelper) =>
    let arr = PgArray(23,
      recover val [as (FieldData | None):
        RawBytes(recover val [as U8: 1; 2] end)] end)
    h.assert_error({()? => _ArrayEncoder(arr)? })
