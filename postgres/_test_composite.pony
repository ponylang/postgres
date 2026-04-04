use "cli"
use "collections"
use lori = "lori"
use "pony_test"

// ============================================================
// Binary composite wire data builder
// ============================================================

primitive \nodoc\ _TestCompositeBinaryBuilder
  """
  Builds binary composite wire data for testing.
  """
  fun apply(fields: Array[(U32, (Array[U8] val | None))] val): Array[U8] val =>
    try
      var data_size: USize = 4  // field_count
      for (_, raw) in fields.values() do
        data_size = data_size + 8  // oid + len
        match raw
        | let b: Array[U8] val => data_size = data_size + b.size()
        end
      end

      recover val
        let msg = Array[U8].init(0, data_size)
        ifdef bigendian then
          msg.update_u32(0, fields.size().u32())?
        else
          msg.update_u32(0, fields.size().u32().bswap())?
        end
        var offset: USize = 4
        for (oid, raw) in fields.values() do
          ifdef bigendian then
            msg.update_u32(offset, oid)?
          else
            msg.update_u32(offset, oid.bswap())?
          end
          offset = offset + 4
          match raw
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
        msg
      end
    else
      _Unreachable()
      recover val Array[U8] end
    end

  fun int4_bytes(v: I32): Array[U8] val =>
    _TestArrayBinaryBuilder.int4_bytes(v)

  fun text_bytes(v: String): Array[U8] val =>
    v.array()

// ============================================================
// PgComposite construction & access tests
// ============================================================

class \nodoc\ iso _TestCompositeCreate is UnitTest
  fun name(): String =>
    "Composite/Create"

  fun apply(h: TestHelper) ? =>
    let c = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    h.assert_eq[USize](2, c.size())
    h.assert_eq[U32](16400, c.type_oid)
    match c(0)?
    | let s: String => h.assert_eq[String]("Alice", s)
    else h.fail("Expected String")
    end
    match c(1)?
    | let v: I32 => h.assert_eq[I32](30, v)
    else h.fail("Expected I32")
    end
    match c.field("name")?
    | let s: String => h.assert_eq[String]("Alice", s)
    else h.fail("Expected String")
    end
    match c.field("age")?
    | let v: I32 => h.assert_eq[I32](30, v)
    else h.fail("Expected I32")
    end

class \nodoc\ iso _TestCompositeFromFields is UnitTest
  fun name(): String =>
    "Composite/FromFields"

  fun apply(h: TestHelper) ? =>
    let c = PgComposite.from_fields(16400,
      recover val
        [as (String, U32, (FieldData | None)):
          ("name", 25, "Alice"); ("age", 23, I32(30))]
      end)
    h.assert_eq[USize](2, c.size())
    h.assert_eq[U32](16400, c.type_oid)
    match c(0)?
    | let s: String => h.assert_eq[String]("Alice", s)
    else h.fail("Expected String")
    end
    match c(1)?
    | let v: I32 => h.assert_eq[I32](30, v)
    else h.fail("Expected I32")
    end
    match c.field("name")?
    | let s: String => h.assert_eq[String]("Alice", s)
    else h.fail("Expected String via name")
    end
    // Verify field_oids are correct
    h.assert_eq[U32](25, c.field_oids(0)?)
    h.assert_eq[U32](23, c.field_oids(1)?)

class \nodoc\ iso _TestCompositeFromFieldsEmpty is UnitTest
  fun name(): String =>
    "Composite/FromFields/Empty"

  fun apply(h: TestHelper) =>
    let c = PgComposite.from_fields(16400,
      recover val Array[(String, U32, (FieldData | None))] end)
    h.assert_eq[USize](0, c.size())

class \nodoc\ iso _TestCompositeCreateValidation is UnitTest
  fun name(): String =>
    "Composite/Create/Validation"

  fun apply(h: TestHelper) =>
    // Mismatched field_oids and field_names sizes
    h.assert_error({()? =>
      PgComposite(16400,
        recover val [as U32: 25] end,
        recover val [as String: "name"; "age"] end,
        recover val [as (FieldData | None): "Alice"] end)?
    })
    // Mismatched field_oids and fields sizes
    h.assert_error({()? =>
      PgComposite(16400,
        recover val [as U32: 25; 23] end,
        recover val [as String: "name"; "age"] end,
        recover val [as (FieldData | None): "Alice"] end)?
    })

class \nodoc\ iso _TestCompositeNamedAccessMiss is UnitTest
  fun name(): String =>
    "Composite/NamedAccess/Miss"

  fun apply(h: TestHelper) =>
    h.assert_error({()? =>
      let c = PgComposite(16400,
        recover val [as U32: 25] end,
        recover val [as String: "name"] end,
        recover val [as (FieldData | None): "Alice"] end)?
      c.field("nonexistent")?
    })

class \nodoc\ iso _TestCompositeEq is UnitTest
  fun name(): String =>
    "Composite/Equality"

  fun apply(h: TestHelper) ? =>
    let a = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    let b = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    h.assert_true(a == b)

    // Different values
    let c = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Bob"; I32(25)] end)?
    h.assert_false(a == c)

    // Different type_oid
    let d = PgComposite(16500,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    h.assert_false(a == d)

class \nodoc\ iso _TestCompositeFieldDataEq is UnitTest
  fun name(): String =>
    "Composite/FieldDataEq"

  fun apply(h: TestHelper) ? =>
    let a = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    let b = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    h.assert_true(a.field_data_eq(b))

    // Non-PgComposite
    let s: FieldData = "not a composite"
    h.assert_false(a.field_data_eq(s))

class \nodoc\ iso _TestCompositeString is UnitTest
  fun name(): String =>
    "Composite/String"

  fun apply(h: TestHelper) ? =>
    // Simple values
    let c = PgComposite(16400,
      recover val [as U32: 25; 25; 23] end,
      recover val [as String: "street"; "city"; "zip"] end,
      recover val [as (FieldData | None):
        "123 Main St"; "Springfield"; I32(62704)] end)?
    h.assert_eq[String]("(\"123 Main St\",Springfield,62704)", c.string())

    // With NULLs
    let c2 = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; None] end)?
    h.assert_eq[String]("(Alice,)", c2.string())

    // Values needing quoting (comma, parens, quotes)
    let c3 = PgComposite(16400,
      recover val [as U32: 25; 25] end,
      recover val [as String: "a"; "b"] end,
      recover val [as (FieldData | None):
        "has,comma"; "has\"quote"] end)?
    h.assert_eq[String]("(\"has,comma\",\"has\"\"quote\")", c3.string())

    // Empty string field (should be quoted)
    let empty: String val = ""
    let c4 = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): empty] end)?
    h.assert_eq[String]("(\"\")", c4.string())

    // Backslash field (should be quoted and doubled)
    let c5 = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "path"] end,
      recover val [as (FieldData | None): "C:\\Users\\file"] end)?
    h.assert_eq[String]("(\"C:\\\\Users\\\\file\")", c5.string())

    // Whitespace field (should be quoted)
    let c6 = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "val"] end,
      recover val [as (FieldData | None): "has space"] end)?
    h.assert_eq[String]("(\"has space\")", c6.string())

// ============================================================
// CodecRegistry.with_composite_type registration tests
// ============================================================

class \nodoc\ iso _TestCompositeRegistration is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType"

  fun apply(h: TestHelper) ? =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("street", 25); ("city", 25); ("zip_code", 23)]
    end
    let r = CodecRegistry.with_composite_type(16400, fields)?
    h.assert_true(r.has_binary_codec(16400))

    // Chaining with with_array_type
    let r2 = r.with_array_type(16401, 16400)?
    h.assert_true(r2.has_binary_codec(16401))

class \nodoc\ iso _TestCompositeRegistrationRejectsEmpty is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsEmpty"

  fun apply(h: TestHelper) =>
    h.assert_error({()? =>
      let fields: Array[(String, U32)] val = recover val
        Array[(String, U32)]
      end
      CodecRegistry.with_composite_type(16400, fields)?
    })

class \nodoc\ iso _TestCompositeRegistrationRejectsExistingOid is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsExistingOid"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    // int4 OID (23) — built-in binary codec
    h.assert_error({()? => CodecRegistry.with_composite_type(23, fields)? })
    // text OID (25) — built-in text codec
    h.assert_error({()? => CodecRegistry.with_composite_type(25, fields)? })

class \nodoc\ iso _TestCompositeRegistrationRejectsEnumOid is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsEnumOid"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    h.assert_error({()? =>
      let r = CodecRegistry.with_enum_type(50000)?
      r.with_composite_type(50000, fields)?
    })

class \nodoc\ iso _TestCompositeRegistrationRejectsArrayOid is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsArrayOid"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    // Built-in array OID
    h.assert_error({()? =>
      CodecRegistry.with_composite_type(1007, fields)?
    })
    // Custom array OID
    h.assert_error({()? =>
      let r = CodecRegistry.with_array_type(50000, 25)?
      r.with_composite_type(50000, fields)?
    })

class \nodoc\ iso _TestCompositeRegistrationRejectsSelfRef is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsSelfRef"

  fun apply(h: TestHelper) =>
    h.assert_error({()? =>
      let fields: Array[(String, U32)] val = recover val
        [as (String, U32): ("name", 25); ("self", 16400)]
      end
      CodecRegistry.with_composite_type(16400, fields)?
    })

class \nodoc\ iso _TestCompositeRegistrationRejectsDuplicate is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/RejectsDuplicate"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    h.assert_error({()? =>
      let r = CodecRegistry.with_composite_type(16400, fields)?
      r.with_composite_type(16400, fields)?
    })

class \nodoc\ iso _TestCompositeRegistrationBlocksWithCodec is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/BlocksWithCodec"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    // with_codec should reject an OID already registered as composite
    h.assert_error({()? =>
      let r = CodecRegistry.with_composite_type(16400, fields)?
      r.with_codec(16400, _TextPassthroughBinaryCodec)?
    })

class \nodoc\ iso _TestCompositeRegistrationBlocksWithEnumType is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/BlocksWithEnumType"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    // with_enum_type should reject an OID already registered as composite
    h.assert_error({()? =>
      let r = CodecRegistry.with_composite_type(16400, fields)?
      r.with_enum_type(16400)?
    })

class \nodoc\ iso _TestCompositeRegistrationBlocksWithArrayType is UnitTest
  fun name(): String =>
    "CodecRegistry/WithCompositeType/BlocksWithArrayType"

  fun apply(h: TestHelper) =>
    let fields: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25)]
    end
    // with_array_type should reject using a composite OID as array_oid
    h.assert_error({()? =>
      let r = CodecRegistry.with_composite_type(16400, fields)?
      r.with_array_type(16400, 25)?
    })

class \nodoc\ iso _TestCompositeEqSizeMismatch is UnitTest
  fun name(): String =>
    "Composite/Equality/SizeMismatch"

  fun apply(h: TestHelper) ? =>
    let a = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    let b = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    h.assert_false(a == b)

class \nodoc\ iso _TestCompositeEqWithNulls is UnitTest
  fun name(): String =>
    "Composite/Equality/WithNulls"

  fun apply(h: TestHelper) ? =>
    let a = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; None] end)?
    let b = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; None] end)?
    h.assert_true(a == b)

// ============================================================
// Binary decode tests
// ============================================================

class \nodoc\ iso _TestBinaryDecodeCompositeSimple is UnitTest
  fun name(): String =>
    "Codec/Binary/Composite/Simple"

  fun apply(h: TestHelper) ? =>
    let fields: Array[(U32, (Array[U8] val | None))] val = recover val
      [as (U32, (Array[U8] val | None)):
        (25, _TestCompositeBinaryBuilder.text_bytes("hello"))
        (23, _TestCompositeBinaryBuilder.int4_bytes(42))]
    end
    let data = _TestCompositeBinaryBuilder(fields)
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 1, data)?
    match result
    | let c: PgComposite =>
      h.assert_eq[USize](2, c.size())
      h.assert_eq[U32](16400, c.type_oid)
      match c(0)?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String")
      end
      match c(1)?
      | let v: I32 => h.assert_eq[I32](42, v)
      else h.fail("Expected I32")
      end
      // Verify field names are accessible
      match c.field("name")?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String via name")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestBinaryDecodeCompositeWithNull is UnitTest
  fun name(): String =>
    "Codec/Binary/Composite/WithNull"

  fun apply(h: TestHelper) ? =>
    let fields: Array[(U32, (Array[U8] val | None))] val = recover val
      [as (U32, (Array[U8] val | None)):
        (25, _TestCompositeBinaryBuilder.text_bytes("hello"))
        (23, None)]
    end
    let data = _TestCompositeBinaryBuilder(fields)
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 1, data)?
    match result
    | let c: PgComposite =>
      h.assert_eq[USize](2, c.size())
      match c(0)?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String")
      end
      match c(1)?
      | None => None  // expected
      else h.fail("Expected None")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestBinaryDecodeCompositeNested is UnitTest
  fun name(): String =>
    "Codec/Binary/Composite/Nested"

  fun apply(h: TestHelper) ? =>
    // Inner composite: (text, int4)
    let inner_fields: Array[(U32, (Array[U8] val | None))] val = recover val
      [as (U32, (Array[U8] val | None)):
        (25, _TestCompositeBinaryBuilder.text_bytes("inner"))
        (23, _TestCompositeBinaryBuilder.int4_bytes(99))]
    end
    let inner_data = _TestCompositeBinaryBuilder(inner_fields)

    // Outer composite: (text, inner_composite)
    let outer_fields: Array[(U32, (Array[U8] val | None))] val = recover val
      [as (U32, (Array[U8] val | None)):
        (25, _TestCompositeBinaryBuilder.text_bytes("outer"))
        (16400, inner_data)]
    end
    let outer_data = _TestCompositeBinaryBuilder(outer_fields)

    let inner_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25); ("b", 23)]
    end
    let outer_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("label", 25); ("nested", 16400)]
    end
    let registry = CodecRegistry
      .with_composite_type(16400, inner_desc)?
      .with_composite_type(16500, outer_desc)?
    let result = registry.decode(16500, 1, outer_data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | let s: String => h.assert_eq[String]("outer", s)
      else h.fail("Expected String")
      end
      match c(1)?
      | let inner: PgComposite =>
        match inner(0)?
        | let s: String => h.assert_eq[String]("inner", s)
        else h.fail("Expected inner String")
        end
        match inner(1)?
        | let v: I32 => h.assert_eq[I32](99, v)
        else h.fail("Expected inner I32")
        end
      else
        h.fail("Expected nested PgComposite")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestBinaryDecodeCompositeFieldCountMismatch is UnitTest
  fun name(): String =>
    "Codec/Binary/Composite/FieldCountMismatch"

  fun apply(h: TestHelper) =>
    // Wire data has 3 fields, registration has 2
    h.assert_error({()? =>
      let fields: Array[(U32, (Array[U8] val | None))] val = recover val
        [as (U32, (Array[U8] val | None)):
          (25, _TestCompositeBinaryBuilder.text_bytes("a"))
          (23, _TestCompositeBinaryBuilder.int4_bytes(1))
          (23, _TestCompositeBinaryBuilder.int4_bytes(2))]
      end
      let data = _TestCompositeBinaryBuilder(fields)
      let descriptors: Array[(String, U32)] val = recover val
        [as (String, U32): ("name", 25); ("age", 23)]
      end
      let registry = CodecRegistry.with_composite_type(16400, descriptors)?
      registry.decode(16400, 1, data)?
    })

class \nodoc\ iso _TestBinaryDecodeCompositeDepthGuard is UnitTest
  fun name(): String =>
    "Codec/Binary/Composite/DepthGuard"

  fun apply(h: TestHelper) =>
    // Build deeply nested composite data that exceeds the depth limit.
    // We register 18 composite types, each nesting the previous one.
    // At decode time, depth should exceed the limit of 16.
    h.assert_error({()? =>
      var registry = CodecRegistry
      // Register composites: OIDs 60000..60017
      // 60000: (text)
      let leaf_desc: Array[(String, U32)] val = recover val
        [as (String, U32): ("val", 25)]
      end
      registry = registry.with_composite_type(60000, leaf_desc)?
      var oid: U32 = 60001
      while oid <= 60017 do
        let inner_oid = oid - 1
        let desc: Array[(String, U32)] val = recover val
          [as (String, U32): ("inner", inner_oid)]
        end
        registry = registry.with_composite_type(oid, desc)?
        oid = oid + 1
      end

      // Build nested wire data from inside out
      var data: Array[U8] val = _TestCompositeBinaryBuilder(
        recover val
          [as (U32, (Array[U8] val | None)):
            (25, "leaf".array())]
        end)
      oid = 60001
      while oid <= 60017 do
        let inner_oid = oid - 1
        data = _TestCompositeBinaryBuilder(
          recover val
            [as (U32, (Array[U8] val | None)): (inner_oid, data)]
          end)
        oid = oid + 1
      end

      // Should exceed depth limit
      registry.decode(60017, 1, data)?
    })

// ============================================================
// Text decode tests
// ============================================================

class \nodoc\ iso _TestTextDecodeCompositeSimple is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/Simple"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "(hello,42)".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      h.assert_eq[USize](2, c.size())
      match c(0)?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String")
      end
      // Text codec for int4 (OID 23) decodes to I32
      match c(1)?
      | let v: I32 => h.assert_eq[I32](42, v)
      else h.fail("Expected I32 from text codec")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeWithNull is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/WithNull"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "(hello,)".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String")
      end
      match c(1)?
      | None => None  // expected
      else h.fail("Expected None for trailing empty field")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeEmptyString is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/EmptyString"

  fun apply(h: TestHelper) ? =>
    let data: Array[U8] val = "(hello,\"\")".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("val", 25)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      match c(1)?
      | let s: String => h.assert_eq[String]("", s)
      else h.fail("Expected empty String")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeQuoted is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/Quoted"

  fun apply(h: TestHelper) ? =>
    // (  "has,comma"  ,  "has""quote"  )
    let data: Array[U8] val =
      "(\"has,comma\",\"has\"\"quote\")".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25); ("b", 25)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | let s: String => h.assert_eq[String]("has,comma", s)
      else h.fail("Expected String a")
      end
      match c(1)?
      | let s: String => h.assert_eq[String]("has\"quote", s)
      else h.fail("Expected String b")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeNested is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/Nested"

  fun apply(h: TestHelper) ? =>
    // Nested composite in text format: the inner composite appears as a
    // quoted string with doubled-double-quote escaping for inner quotes.
    // PostgreSQL renders: ("(inner_val,99)",outer_label)
    let inner_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25); ("b", 23)]
    end
    let outer_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("nested", 16400); ("label", 25)]
    end
    let registry = CodecRegistry
      .with_composite_type(16400, inner_desc)?
      .with_composite_type(16500, outer_desc)?

    // Outer composite text: nested field is quoted, inner parens preserved
    let data: Array[U8] val = "(\"(inner_val,99)\",outer_label)".array()
    let result = registry.decode(16500, 0, data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | let inner: PgComposite =>
        match inner(0)?
        | let s: String => h.assert_eq[String]("inner_val", s)
        else h.fail("Expected inner String a")
        end
        // Text codec for int4 (OID 23) decodes to I32
        match inner(1)?
        | let v: I32 => h.assert_eq[I32](99, v)
        else h.fail("Expected inner I32 b")
        end
      else
        h.fail("Expected nested PgComposite")
      end
      match c(1)?
      | let s: String => h.assert_eq[String]("outer_label", s)
      else h.fail("Expected String label")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeBackslash is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/Backslash"

  fun apply(h: TestHelper) ? =>
    // PostgreSQL doubles backslashes in composite text output:
    // C:\Users\file → ("C:\\Users\\file")
    let data: Array[U8] val = "(\"C:\\\\Users\\\\file\")".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("path", 25)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | let s: String => h.assert_eq[String]("C:\\Users\\file", s)
      else h.fail("Expected String")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeFieldCountMismatch is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/FieldCountMismatch"

  fun apply(h: TestHelper) =>
    // 3 fields in text but only 2 registered
    h.assert_error({()? =>
      let data: Array[U8] val = "(a,b,c)".array()
      let descriptors: Array[(String, U32)] val = recover val
        [as (String, U32): ("x", 25); ("y", 25)]
      end
      let registry = CodecRegistry.with_composite_type(16400, descriptors)?
      registry.decode(16400, 0, data)?
    })

class \nodoc\ iso _TestTextDecodeCompositeNullFirst is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/NullFirst"

  fun apply(h: TestHelper) ? =>
    // Leading NULL field
    let data: Array[U8] val = "(,hello)".array()
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("a", 25); ("b", 25)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?
    let result = registry.decode(16400, 0, data)?
    match result
    | let c: PgComposite =>
      match c(0)?
      | None => None  // expected
      else h.fail("Expected None for first field")
      end
      match c(1)?
      | let s: String => h.assert_eq[String]("hello", s)
      else h.fail("Expected String")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestTextDecodeCompositeFewerFields is UnitTest
  fun name(): String =>
    "Codec/Text/Composite/FewerFields"

  fun apply(h: TestHelper) =>
    // 1 field in text, 2 registered — schema drift, should error
    h.assert_error({()? =>
      let data: Array[U8] val = "(hello)".array()
      let descriptors: Array[(String, U32)] val = recover val
        [as (String, U32): ("a", 25); ("b", 25)]
      end
      let registry = CodecRegistry.with_composite_type(16400, descriptors)?
      registry.decode(16400, 0, data)?
    })

// ============================================================
// Encode tests
// ============================================================

class \nodoc\ iso _TestCompositeEncoderSimple is UnitTest
  fun name(): String =>
    "Composite/Encoder/Simple"

  fun apply(h: TestHelper) ? =>
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?

    let c = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?

    // Encode then decode — roundtrip
    let encoded = _CompositeEncoder(c, registry)?
    let decoded = registry.decode(16400, 1, encoded)?
    match decoded
    | let d: PgComposite =>
      h.assert_eq[USize](2, d.size())
      match d(0)?
      | let s: String => h.assert_eq[String]("Alice", s)
      else h.fail("Expected String")
      end
      match d(1)?
      | let v: I32 => h.assert_eq[I32](30, v)
      else h.fail("Expected I32")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderWithNull is UnitTest
  fun name(): String =>
    "Composite/Encoder/WithNull"

  fun apply(h: TestHelper) ? =>
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?

    let c = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; None] end)?

    let encoded = _CompositeEncoder(c, registry)?
    let decoded = registry.decode(16400, 1, encoded)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let s: String => h.assert_eq[String]("Alice", s)
      else h.fail("Expected String")
      end
      match d(1)?
      | None => None  // expected
      else h.fail("Expected None")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderNested is UnitTest
  fun name(): String =>
    "Composite/Encoder/Nested"

  fun apply(h: TestHelper) ? =>
    let inner_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("val", 25)]
    end
    let outer_desc: Array[(String, U32)] val = recover val
      [as (String, U32): ("label", 25); ("inner", 16400)]
    end
    let registry = CodecRegistry
      .with_composite_type(16400, inner_desc)?
      .with_composite_type(16500, outer_desc)?

    let inner = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "val"] end,
      recover val [as (FieldData | None): "nested_val"] end)?

    let outer = PgComposite(16500,
      recover val [as U32: 25; 16400] end,
      recover val [as String: "label"; "inner"] end,
      recover val [as (FieldData | None): "top"; inner] end)?

    let encoded = _CompositeEncoder(outer, registry)?
    let decoded = registry.decode(16500, 1, encoded)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let s: String => h.assert_eq[String]("top", s)
      else h.fail("Expected String")
      end
      match d(1)?
      | let inner_d: PgComposite =>
        match inner_d(0)?
        | let s: String => h.assert_eq[String]("nested_val", s)
        else h.fail("Expected inner String")
        end
      else
        h.fail("Expected nested PgComposite")
      end
    else
      h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderI16Roundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/I16/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 21)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 21] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): I16(-5)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: I16 => h.assert_eq[I16](-5, v)
      else h.fail("Expected I16")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderI64Roundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/I64/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 20)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 20] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): I64(1000000)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: I64 => h.assert_eq[I64](1000000, v)
      else h.fail("Expected I64")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderF32Roundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/F32/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 700)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 700] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): F32(3.14)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: F32 => h.assert_eq[F32](F32(3.14), v)
      else h.fail("Expected F32")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderF64Roundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/F64/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 701)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 701] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): F64(2.718281828)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: F64 => h.assert_eq[F64](F64(2.718281828), v)
      else h.fail("Expected F64")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderBoolRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Bool/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 16)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 16] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): true] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: Bool => h.assert_true(v)
      else h.fail("Expected Bool")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderByteaRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Bytea/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 17)] end)?
    let data: Array[U8] val = [as U8: 0xDE; 0xAD; 0xBE; 0xEF]
    let c = PgComposite(16400,
      recover val [as U32: 17] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): Bytea(data)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: Bytea => h.assert_eq[Bytea](Bytea(data), v)
      else h.fail("Expected Bytea")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderDateRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Date/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 1082)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 1082] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): PgDate(8765)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: PgDate => h.assert_eq[PgDate](PgDate(8765), v)
      else h.fail("Expected PgDate")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderTimeRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Time/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 1083)] end)?
    let us = MakePgTimeMicroseconds(43200000000)
    let t = match us
    | let valid: PgTimeMicroseconds => PgTime(valid)
    else
      h.fail("Invalid time")
      return
    end
    let c = PgComposite(16400,
      recover val [as U32: 1083] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): t] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: PgTime => h.assert_eq[PgTime](t, v)
      else h.fail("Expected PgTime")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderTimestampRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Timestamp/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 1114)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 1114] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): PgTimestamp(694224000000000)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: PgTimestamp =>
        h.assert_eq[PgTimestamp](PgTimestamp(694224000000000), v)
      else h.fail("Expected PgTimestamp")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderIntervalRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Interval/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 1186)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 1186] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): PgInterval(3600000000, 5, 2)] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: PgInterval =>
        h.assert_eq[PgInterval](PgInterval(3600000000, 5, 2), v)
      else h.fail("Expected PgInterval")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderUuidRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Uuid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 2950)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 2950] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None):
        "550e8400-e29b-41d4-a716-446655440000"] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: String =>
        h.assert_eq[String]("550e8400-e29b-41d4-a716-446655440000", v)
      else h.fail("Expected String (uuid)")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderJsonbRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Jsonb/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 3802)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 3802] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): "{\"key\":\"value\"}"] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: String => h.assert_eq[String]("{\"key\":\"value\"}", v)
      else h.fail("Expected String (jsonb)")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderOidRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Oid/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 26)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 26] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): "12345"] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: String => h.assert_eq[String]("12345", v)
      else h.fail("Expected String (oid)")
      end
    else h.fail("Expected PgComposite")
    end

class \nodoc\ iso _TestCompositeEncoderNumericRoundtrip is UnitTest
  fun name(): String =>
    "Composite/Encoder/Numeric/Roundtrip"

  fun apply(h: TestHelper) ? =>
    let registry = CodecRegistry.with_composite_type(16400,
      recover val [as (String, U32): ("v", 1700)] end)?
    let c = PgComposite(16400,
      recover val [as U32: 1700] end,
      recover val [as String: "v"] end,
      recover val [as (FieldData | None): "123456.789"] end)?
    let decoded = registry.decode(16400, 1, _CompositeEncoder(c, registry)?)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let v: String => h.assert_eq[String]("123456.789", v)
      else h.fail("Expected String (numeric)")
      end
    else h.fail("Expected PgComposite")
    end

// ============================================================
// _ParamEncoder tests
// ============================================================

class \nodoc\ iso _TestParamEncoderComposite is UnitTest
  fun name(): String =>
    "ParamEncoder/Composite"

  fun apply(h: TestHelper) ? =>
    let c = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: c]
    end
    let oids = _ParamEncoder.oids_for(params, CodecRegistry)
    h.assert_eq[U32](16400, oids(0)?)

// ============================================================
// _FrontendMessage.bind() tests
// ============================================================

class \nodoc\ iso _TestFrontendMessageBindWithComposite is UnitTest
  fun name(): String =>
    "FrontendMessage/Bind/Composite"

  fun apply(h: TestHelper) ? =>
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("age", 23)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?

    let c = PgComposite(16400,
      recover val [as U32: 25; 23] end,
      recover val [as String: "name"; "age"] end,
      recover val [as (FieldData | None): "Alice"; I32(30)] end)?
    let params: Array[FieldDataTypes] val = recover val
      [as FieldDataTypes: c]
    end

    // Should not error — bind produces valid wire data
    let result = _FrontendMessage.bind("", "", params, registry)?
    // Verify we got some bytes (basic sanity)
    h.assert_true(result.size() > 0)

// ============================================================
// Composite-in-array and array-in-composite tests
// ============================================================

class \nodoc\ iso _TestArrayWithCompositeElements is UnitTest
  fun name(): String =>
    "Composite/ArrayWithCompositeElements"

  fun apply(h: TestHelper) ? =>
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25)]
    end
    let registry = CodecRegistry
      .with_composite_type(16400, descriptors)?
      .with_array_type(16401, 16400)?

    let c1 = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    let c2 = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Bob"] end)?

    let arr = PgArray(16400,
      recover val [as (FieldData | None): c1; c2] end)

    // Encode then decode roundtrip
    let encoded = _ArrayEncoder(arr, registry)?
    let decoded = registry.decode(16401, 1, encoded)?
    match decoded
    | let a: PgArray =>
      h.assert_eq[USize](2, a.size())
      match a(0)?
      | let d: PgComposite =>
        match d(0)?
        | let s: String => h.assert_eq[String]("Alice", s)
        else h.fail("Expected String")
        end
      else
        h.fail("Expected PgComposite")
      end
      match a(1)?
      | let d: PgComposite =>
        match d(0)?
        | let s: String => h.assert_eq[String]("Bob", s)
        else h.fail("Expected String")
        end
      else
        h.fail("Expected PgComposite")
      end
    else
      h.fail("Expected PgArray")
    end

class \nodoc\ iso _TestCompositeWithArrayField is UnitTest
  fun name(): String =>
    "Composite/WithArrayField"

  fun apply(h: TestHelper) ? =>
    // Composite with an int4[] field
    let descriptors: Array[(String, U32)] val = recover val
      [as (String, U32): ("name", 25); ("scores", 1007)]
    end
    let registry = CodecRegistry.with_composite_type(16400, descriptors)?

    let scores = PgArray(23,
      recover val [as (FieldData | None): I32(10); I32(20)] end)
    let c = PgComposite(16400,
      recover val [as U32: 25; 1007] end,
      recover val [as String: "name"; "scores"] end,
      recover val [as (FieldData | None): "Alice"; scores] end)?

    let encoded = _CompositeEncoder(c, registry)?
    let decoded = registry.decode(16400, 1, encoded)?
    match decoded
    | let d: PgComposite =>
      match d(0)?
      | let s: String => h.assert_eq[String]("Alice", s)
      else h.fail("Expected String")
      end
      match d(1)?
      | let a: PgArray =>
        h.assert_eq[USize](2, a.size())
        match a(0)?
        | let v: I32 => h.assert_eq[I32](10, v)
        else h.fail("Expected I32")
        end
      else
        h.fail("Expected PgArray")
      end
    else
      h.fail("Expected PgComposite")
    end

// ============================================================
// _FieldDataEq tests
// ============================================================

class \nodoc\ iso _TestFieldDataEqComposite is UnitTest
  fun name(): String =>
    "FieldDataEq/Composite"

  fun apply(h: TestHelper) ? =>
    let a: FieldData = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    let b: FieldData = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Alice"] end)?
    h.assert_true(_FieldDataEq(a, b))

    let c: FieldData = PgComposite(16400,
      recover val [as U32: 25] end,
      recover val [as String: "name"] end,
      recover val [as (FieldData | None): "Bob"] end)?
    h.assert_false(_FieldDataEq(a, c))

// ============================================================
// Unregistered composite OID fallback tests
// ============================================================

class \nodoc\ iso _TestCompositeUnregisteredFallback is UnitTest
  fun name(): String =>
    "Codec/Composite/UnregisteredFallback"

  fun apply(h: TestHelper) ? =>
    // Binary format — unregistered composite OID falls back to RawBytes
    let fields: Array[(U32, (Array[U8] val | None))] val = recover val
      [as (U32, (Array[U8] val | None)):
        (25, _TestCompositeBinaryBuilder.text_bytes("hello"))]
    end
    let data = _TestCompositeBinaryBuilder(fields)
    let registry = CodecRegistry
    let binary_result = registry.decode(99999, 1, data)?
    match binary_result
    | let rb: RawBytes => None // expected — unregistered binary falls back
    else h.fail("Expected RawBytes for unregistered binary OID")
    end

    // Text format — unregistered composite OID falls back to String
    let text_data: Array[U8] val = "(hello,42)".array()
    let text_result = registry.decode(99999, 0, text_data)?
    match text_result
    | let s: String => None // expected — unregistered text falls back
    else h.fail("Expected String for unregistered text OID")
    end

// ============================================================
// Integration tests (require running PostgreSQL containers)
// ============================================================

class \nodoc\ iso _TestIntegrationCompositeSelectBinary is UnitTest
  """
  CREATE TYPE, SELECT composite literal via PreparedQuery (binary decode).
  Uses a single session. The composite OID is unknown to the default
  CodecRegistry, so the binary result falls back to RawBytes. This test
  verifies the fallback and the type creation flow.
  """
  fun name(): String =>
    "integration/Composite/SelectBinary"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _CompositeSelectClient(h, true)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(15_000_000_000)

class \nodoc\ iso _TestIntegrationCompositeSelectText is UnitTest
  """
  SELECT composite literal via SimpleQuery (text decode).
  """
  fun name(): String =>
    "integration/Composite/SelectText"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    let client = _CompositeSelectClient(h, false)
    let session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), info.host, info.port),
      DatabaseConnectInfo(info.username, info.password, info.database),
      client)
    h.dispose_when_done(session)
    h.long_test(15_000_000_000)

class \nodoc\ iso _TestIntegrationCompositeRoundtrip is UnitTest
  """
  Two-phase: discover composite OID, then reconnect with registered
  CodecRegistry, INSERT PgComposite, SELECT back and verify decode.
  """
  fun name(): String =>
    "integration/Composite/Roundtrip"

  fun apply(h: TestHelper) =>
    let info = _ConnectionTestConfiguration(h.env.vars)
    _CompositeRoundtripClient(h, info)
    h.long_test(15_000_000_000)

// Single-session client: create type, SELECT, verify result type, drop type.
// Since the session's CodecRegistry doesn't know the composite OID,
// PreparedQuery returns RawBytes and SimpleQuery returns String — both
// are valid fallback behaviors. This test verifies the end-to-end flow.
actor \nodoc\ _CompositeSelectClient is
  (SessionStatusNotify & ResultReceiver)

  let _h: TestHelper
  let _use_prepared: Bool
  var _phase: USize = 0

  new create(h: TestHelper, use_prepared: Bool) =>
    _h = h
    _use_prepared = use_prepared

  be pg_session_authenticated(session: Session) =>
    session.execute(
      SimpleQuery("DROP TYPE IF EXISTS _test_address_sel"), this)

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)
    s.close()

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1
    match _phase
    | 1 =>
      session.execute(SimpleQuery(
        "CREATE TYPE _test_address_sel AS (street text, city text, zip int4)"),
        this)
    | 2 =>
      if _use_prepared then
        session.execute(PreparedQuery(
          "SELECT ROW('123 Main','Springfield',62704)::_test_address_sel",
          recover val Array[FieldDataTypes] end), this)
      else
        session.execute(SimpleQuery(
          "SELECT ROW('123 Main','Springfield',62704)::_test_address_sel"),
          this)
      end
    | 3 =>
      // Verify we got a result (fallback type since OID not registered)
      match result
      | let rs: ResultSet =>
        try
          let field = rs.rows()(0)?.fields(0)?
          if _use_prepared then
            // Binary format, unregistered OID → RawBytes
            match field.value
            | let _: RawBytes => None // expected
            else _h.fail("Expected RawBytes for unregistered binary composite")
            end
          else
            // Text format, unregistered OID → String
            match field.value
            | let _: String => None // expected
            else _h.fail("Expected String for unregistered text composite")
            end
          end
        else
          _h.fail("Row access error")
        end
      else
        _h.fail("Expected ResultSet")
      end
      session.execute(SimpleQuery("DROP TYPE _test_address_sel"), this)
    | 4 =>
      _h.complete(true)
      session.close()
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let e: ErrorResponseMessage =>
      _h.fail("Query failed: " + e.message)
    else
      _h.fail("Query failed: client error")
    end
    _h.complete(false)
    session.close()

// Two-phase roundtrip client: phase 1 creates type and discovers OID,
// phase 2 reconnects with registered CodecRegistry, inserts, selects back.
actor \nodoc\ _CompositeRoundtripClient is
  (SessionStatusNotify & ResultReceiver)

  let _h: TestHelper
  let _info: _ConnectionTestConfiguration
  var _session: Session
  var _phase: USize = 0
  var _composite_oid: U32 = 0

  new create(h: TestHelper, info: _ConnectionTestConfiguration) =>
    _h = h
    _info = info
    _session = Session(
      ServerConnectInfo(lori.TCPConnectAuth(h.env.root), _info.host,
        _info.port),
      DatabaseConnectInfo(_info.username, _info.password, _info.database),
      this)

  be pg_session_authenticated(session: Session) =>
    _session = session
    match _phase
    | 0 =>
      session.execute(
        SimpleQuery("DROP TABLE IF EXISTS _test_comp_rt"), this)
    | 5 =>
      _phase = 6
      let addr = PgComposite.from_fields(_composite_oid,
        recover val
          [as (String, U32, (FieldData | None)):
            ("street", 25, "42 Elm St")
            ("city", 25, "Portland")
            ("zip", 23, I32(97201))]
        end)
      session.execute(PreparedQuery(
        "INSERT INTO _test_comp_rt (addr) VALUES ($1)",
        recover val [as FieldDataTypes: addr] end), this)
    end

  be pg_session_authentication_failed(
    s: Session,
    reason: AuthenticationFailureReason)
  =>
    _h.fail("Authentication failed")
    _h.complete(false)
    s.close()

  be pg_query_result(session: Session, result: Result) =>
    _phase = _phase + 1
    match _phase
    | 1 =>
      session.execute(
        SimpleQuery("DROP TYPE IF EXISTS _test_address_rt"), this)
    | 2 =>
      session.execute(SimpleQuery(
        "CREATE TYPE _test_address_rt AS (street text, city text, zip int4)"),
        this)
    | 3 =>
      session.execute(SimpleQuery(
        "CREATE TABLE _test_comp_rt (id serial PRIMARY KEY, addr _test_address_rt)"),
        this)
    | 4 =>
      session.execute(SimpleQuery(
        "SELECT oid FROM pg_type WHERE typname = '_test_address_rt'"), this)
    | 5 =>
      match result
      | let rs: ResultSet =>
        try
          match rs.rows()(0)?.fields(0)?.value
          | let s: String => _composite_oid = s.u32()?
          end
        end
      end
      if _composite_oid == 0 then
        _h.fail("Failed to discover composite OID")
        _h.complete(false)
        session.close()
        return
      end
      let descriptors: Array[(String, U32)] val = recover val
        [as (String, U32): ("street", 25); ("city", 25); ("zip", 23)]
      end
      let registry = try
        CodecRegistry.with_composite_type(_composite_oid, descriptors)?
      else
        _h.fail("Failed to register composite type")
        _h.complete(false)
        session.close()
        return
      end
      session.close()
      _session = Session(
        ServerConnectInfo(lori.TCPConnectAuth(_h.env.root), _info.host,
          _info.port),
        DatabaseConnectInfo(_info.username, _info.password, _info.database),
        this where registry = registry)
    | 7 =>
      session.execute(PreparedQuery(
        "SELECT addr FROM _test_comp_rt",
        recover val Array[FieldDataTypes] end), this)
    | 8 =>
      match result
      | let rs: ResultSet =>
        try
          match rs.rows()(0)?.fields(0)?.value
          | let c: PgComposite =>
            match c.field("street")?
            | let s: String => _h.assert_eq[String]("42 Elm St", s)
            else _h.fail("Expected String for street")
            end
            match c.field("city")?
            | let s: String => _h.assert_eq[String]("Portland", s)
            else _h.fail("Expected String for city")
            end
            match c.field("zip")?
            | let v: I32 => _h.assert_eq[I32](97201, v)
            else _h.fail("Expected I32 for zip")
            end
          else
            _h.fail("Expected PgComposite")
            _h.complete(false)
            session.close()
            return
          end
        else
          _h.fail("Row access error")
          _h.complete(false)
          session.close()
          return
        end
      else
        _h.fail("Expected ResultSet")
        _h.complete(false)
        session.close()
        return
      end
      session.execute(SimpleQuery(
        "DROP TABLE _test_comp_rt; DROP TYPE _test_address_rt"), this)
    | 9 =>
      _h.complete(true)
      session.close()
    end

  be pg_query_failed(session: Session, query: Query,
    failure: (ErrorResponseMessage | ClientQueryError))
  =>
    match failure
    | let _: SessionClosed => return
    | let e: ErrorResponseMessage =>
      _h.fail("Query failed: " + e.message)
    else
      _h.fail("Query failed: client error")
    end
    _h.complete(false)
    session.close()
