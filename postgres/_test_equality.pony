use "collections"
use "constrained_types"
use "pony_check"
use "pony_test"

class \nodoc\ iso _TestFieldEqualityReflexive is UnitTest
  """
  Every built-in FieldData type produces a Field that is equal to itself.
  Covers all 14 built-in types to verify each match branch in Field.eq.
  """
  fun name(): String => "Field/Equality/Reflexive"

  fun apply(h: TestHelper) ? =>
    let fields: Array[Field] val = [
      Field("bytes", Bytea(recover val [as U8: 1; 2; 3] end))
      Field("raw", RawBytes(recover val [as U8: 4; 5; 6] end))
      Field("b", true)
      Field("f32", F32(1.5))
      Field("f64", F64(2.5))
      Field("i16", I16(16))
      Field("i32", I32(32))
      Field("i64", I64(64))
      Field("none", None)
      Field("date", PgDate(8765))
      Field("interval", PgInterval(3_600_000_000, 1, 2))
      Field("time",
        PgTime(MakePgTimeMicroseconds(52_200_000_000) as PgTimeMicroseconds))
      Field("timestamp", PgTimestamp(788_918_400_000_000))
      Field("str", "hello")
    ]
    for f in fields.values() do
      h.assert_true(f == f)
    end

class \nodoc\ iso _TestFieldEqualityStructural is UnitTest
  """
  Two independently constructed Fields with the same name and value are equal.
  Covers all 14 built-in FieldData types.
  """
  fun name(): String => "Field/Equality/Structural"

  fun apply(h: TestHelper) ? =>
    h.assert_true(
      Field("a", Bytea(recover val [as U8: 1; 2] end))
        == Field("a", Bytea(recover val [as U8: 1; 2] end)))
    h.assert_true(Field("a", true) == Field("a", true))
    h.assert_true(Field("a", F32(1.5)) == Field("a", F32(1.5)))
    h.assert_true(Field("a", F64(2.5)) == Field("a", F64(2.5)))
    h.assert_true(Field("a", I16(16)) == Field("a", I16(16)))
    h.assert_true(Field("a", I32(32)) == Field("a", I32(32)))
    h.assert_true(Field("a", I64(64)) == Field("a", I64(64)))
    h.assert_true(Field("a", None) == Field("a", None))
    h.assert_true(Field("a", PgDate(100)) == Field("a", PgDate(100)))
    h.assert_true(
      Field("a", PgInterval(1000, 2, 3))
        == Field("a", PgInterval(1000, 2, 3)))
    h.assert_true(
      Field("a",
        PgTime(MakePgTimeMicroseconds(1000) as PgTimeMicroseconds))
        == Field("a",
        PgTime(MakePgTimeMicroseconds(1000) as PgTimeMicroseconds)))
    h.assert_true(
      Field("a", PgTimestamp(1000)) == Field("a", PgTimestamp(1000)))
    h.assert_true(Field("a", "hello") == Field("a", "hello"))
    h.assert_true(
      Field("a", RawBytes(recover val [as U8: 7; 8] end))
        == Field("a", RawBytes(recover val [as U8: 7; 8] end)))

class \nodoc\ iso _TestFieldEqualitySymmetric is UnitTest
  """
  Field equality is symmetric: if a == b then b == a, and if a != b then
  b != a. Tests across different value types.
  """
  fun name(): String => "Field/Equality/Symmetric"

  fun apply(h: TestHelper) =>
    // Equal pairs: a == b and b == a
    let f1 = Field("x", I32(42))
    let f2 = Field("x", I32(42))
    h.assert_true(f1.eq(f2) == f2.eq(f1))

    // Unequal pairs: different types
    let f3 = Field("x", I32(42))
    let f4 = Field("x", "42")
    h.assert_true(f3.eq(f4) == f4.eq(f3))

    // Unequal pairs: different names
    let f5 = Field("x", I32(42))
    let f6 = Field("y", I32(42))
    h.assert_true(f5.eq(f6) == f6.eq(f5))

    // None vs non-None
    let f7 = Field("x", None)
    let f8 = Field("x", I32(0))
    h.assert_true(f7.eq(f8) == f8.eq(f7))

    // Bytea vs String
    let f9 = Field("x", Bytea(recover val [as U8: 1; 2] end))
    let f10 = Field("x", "hello")
    h.assert_true(f9.eq(f10) == f10.eq(f9))

class \nodoc\ iso _TestFieldInequality is UnitTest
  fun name(): String => "Field/Inequality"

  fun apply(h: TestHelper) ? =>
    // Different names, same value
    h.assert_false(Field("a", I32(42)) == Field("b", I32(42)))

    // Same name, different values of same type
    h.assert_false(Field("a", I32(42)) == Field("a", I32(43)))
    h.assert_false(Field("a", "hello") == Field("a", "world"))
    h.assert_false(Field("a", true) == Field("a", false))

    // Same name, different value types
    h.assert_false(Field("a", I32(42)) == Field("a", "42"))
    h.assert_false(Field("a", I32(42)) == Field("a", I64(42)))
    h.assert_false(Field("a", F32(1.0)) == Field("a", F64(1.0)))
    h.assert_false(Field("a", I16(1)) == Field("a", I32(1)))

    // None vs non-None
    h.assert_false(Field("a", None) == Field("a", I32(0)))
    h.assert_false(Field("a", I32(0)) == Field("a", None))

    // Bytea vs different Bytea
    h.assert_false(
      Field("a", Bytea(recover val [as U8: 1; 2] end))
        == Field("a", Bytea(recover val [as U8: 1; 3] end)))
    h.assert_false(
      Field("a", Bytea(recover val [as U8: 1; 2] end))
        == Field("a", Bytea(recover val [as U8: 1; 2; 3] end)))

    // Bytea vs RawBytes (same underlying bytes, different types)
    h.assert_false(
      Field("a", Bytea(recover val [as U8: 1; 2] end))
        == Field("a", RawBytes(recover val [as U8: 1; 2] end)))
    h.assert_false(
      Field("a", RawBytes(recover val [as U8: 1; 2] end))
        == Field("a", Bytea(recover val [as U8: 1; 2] end)))

    // Bytea vs String
    h.assert_false(
      Field("a", Bytea(recover val [as U8: 1; 2] end))
        == Field("a", "hello"))

    // Temporal type cross-type inequality
    h.assert_false(Field("a", PgDate(0)) == Field("a", I32(0)))
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds))
        == Field("a", I64(0)))
    h.assert_false(Field("a", PgTimestamp(0)) == Field("a", I64(0)))
    h.assert_false(Field("a", PgDate(0)) == Field("a", PgTimestamp(0)))
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(0) as PgTimeMicroseconds))
        == Field("a", PgTimestamp(0)))
    h.assert_false(
      Field("a", PgInterval(0, 0, 0)) == Field("a", PgTimestamp(0)))

    // Temporal type same-type inequality
    h.assert_false(Field("a", PgDate(1)) == Field("a", PgDate(2)))
    h.assert_false(
      Field("a",
        PgTime(MakePgTimeMicroseconds(1) as PgTimeMicroseconds))
        == Field("a",
        PgTime(MakePgTimeMicroseconds(2) as PgTimeMicroseconds)))
    h.assert_false(
      Field("a", PgTimestamp(1)) == Field("a", PgTimestamp(2)))
    h.assert_false(
      Field("a", PgInterval(1, 0, 0)) == Field("a", PgInterval(2, 0, 0)))


class \nodoc\ iso _TestRowEquality is UnitTest
  fun name(): String => "Row/Equality"

  fun apply(h: TestHelper) =>
    // Empty rows are equal
    let empty1 = Row(recover val Array[Field] end)
    let empty2 = Row(recover val Array[Field] end)
    h.assert_true(empty1 == empty2)

    // Reflexive
    let r1 = Row(recover val
      [Field("a", I32(1)); Field("b", "hello")]
    end)
    h.assert_true(r1 == r1)

    // Structural equality: same content, independent construction
    let r2 = Row(recover val
      [Field("a", I32(1)); Field("b", "hello")]
    end)
    let r3 = Row(recover val
      [Field("a", I32(1)); Field("b", "hello")]
    end)
    h.assert_true(r2 == r3)

class \nodoc\ iso _TestRowInequality is UnitTest
  fun name(): String => "Row/Inequality"

  fun apply(h: TestHelper) =>
    // Different sizes
    let r1 = Row(recover val [Field("a", I32(1))] end)
    let r2 = Row(recover val
      [Field("a", I32(1)); Field("b", I32(2))]
    end)
    h.assert_false(r1 == r2)

    // Same size, different content
    let r3 = Row(recover val [Field("a", I32(1))] end)
    let r4 = Row(recover val [Field("a", I32(2))] end)
    h.assert_false(r3 == r4)

class \nodoc\ iso _TestRowsEquality is UnitTest
  fun name(): String => "Rows/Equality"

  fun apply(h: TestHelper) =>
    // Empty Rows are equal
    let empty1 = Rows(recover val Array[Row] end)
    let empty2 = Rows(recover val Array[Row] end)
    h.assert_true(empty1 == empty2)

    // Reflexive
    let rs1 = Rows(recover val
      [Row(recover val [Field("a", I32(1))] end)]
    end)
    h.assert_true(rs1 == rs1)

    // Structural equality
    let rs2 = Rows(recover val
      [Row(recover val [Field("a", I32(1))] end)]
    end)
    let rs3 = Rows(recover val
      [Row(recover val [Field("a", I32(1))] end)]
    end)
    h.assert_true(rs2 == rs3)

class \nodoc\ iso _TestRowsInequality is UnitTest
  fun name(): String => "Rows/Inequality"

  fun apply(h: TestHelper) =>
    // Different sizes
    let rs1 = Rows(recover val
      [Row(recover val [Field("a", I32(1))] end)]
    end)
    let rs2 = Rows(recover val Array[Row] end)
    h.assert_false(rs1 == rs2)

    // Different content
    let rs3 = Rows(recover val
      [Row(recover val [Field("a", I32(1))] end)]
    end)
    let rs4 = Rows(recover val
      [Row(recover val [Field("a", I32(2))] end)]
    end)
    h.assert_false(rs3 == rs4)

// -- Generators --

primitive \nodoc\ _FieldDataGen
  fun apply(): Generator[FieldData] =>
    Generators.frequency[FieldData]([
      (1, Generator[FieldData](object is GenObj[FieldData]
        fun generate(rnd: Randomness): FieldData =>
          let size = rnd.usize(0, 10)
          Bytea(recover val
            let arr = Array[U8](size)
            for _ in Range(0, size) do
              arr.push(rnd.u8())
            end
            arr
          end)
      end))
      (1, Generators.bool().map[FieldData]({(v) => v }))
      (1, Generators.i32().map[FieldData]({(v) => F32.from[I32](v) }))
      (1, Generators.i64().map[FieldData]({(v) => F64.from[I64](v) }))
      (1, Generators.i16().map[FieldData]({(v) => v }))
      (1, Generators.i32().map[FieldData]({(v) => v }))
      (1, Generators.i64().map[FieldData]({(v) => v }))
      (1, Generators.unit[None](None).map[FieldData]({(v) => v }))
      (1, Generators.i32().map[FieldData]({(v) => PgDate(v) }))
      (1, Generators.i64().map[FieldData](
        {(v) => PgInterval(v, v.i32(), (v >> 32).i32()) }))
      (1, Generators.i64().map[FieldData](
        {(v) =>
          try
            PgTime(MakePgTimeMicroseconds((v.abs() % 86_400_000_000).i64())
              as PgTimeMicroseconds)
          else _Unreachable(); I64(0)
          end }))
      (1, Generators.i64().map[FieldData]({(v) => PgTimestamp(v) }))
      (1, Generators.ascii_printable(0, 20)
        .map[FieldData]({(v) => v }))
      (1, Generator[FieldData](object is GenObj[FieldData]
        fun generate(rnd: Randomness): FieldData =>
          let size = rnd.usize(0, 10)
          RawBytes(recover val
            let arr = Array[U8](size)
            for _ in Range(0, size) do
              arr.push(rnd.u8())
            end
            arr
          end)
      end))
    ])

primitive \nodoc\ _FieldGen
  fun apply(): Generator[Field] =>
    Generators.map2[String, FieldData, Field](
      Generators.ascii_printable(1, 10),
      _FieldDataGen(),
      {(name, value) => Field(name, value) })

primitive \nodoc\ _RowGen
  fun apply(): Generator[Row] =>
    Generator[Row](object is GenObj[Row]
      fun generate(rnd: Randomness): Row =>
        let size = rnd.usize(0, 5)
        let fields = recover iso Array[Field](size) end
        for i in Range(0, size) do
          fields.push(Field("f" + i.string(),
            _random_field_value(rnd)))
        end
        Row(consume fields)

      fun _random_field_value(rnd: Randomness): FieldData =>
        match rnd.usize(0, 13)
        | 0 => rnd.bool()
        | 1 => F32.from[I32](rnd.i32())
        | 2 => F64.from[I64](rnd.i64())
        | 3 => rnd.i16()
        | 4 => rnd.i32()
        | 5 => rnd.i64()
        | 6 => None
        | 7 =>
          Bytea(recover val
            let arr = Array[U8](rnd.usize(0, 10))
            for _ in Range(0, arr.space()) do
              arr.push(rnd.u8())
            end
            arr
          end)
        | 8 => PgDate(rnd.i32())
        | 9 => PgInterval(rnd.i64(), rnd.i32(), rnd.i32())
        | 10 =>
            try
              PgTime(MakePgTimeMicroseconds(
                (rnd.i64().abs() % 86_400_000_000).i64())
                as PgTimeMicroseconds)
            else _Unreachable(); I64(0)
            end
        | 11 => PgTimestamp(rnd.i64())
        | 12 =>
          RawBytes(recover val
            let arr = Array[U8](rnd.usize(0, 10))
            for _ in Range(0, arr.space()) do
              arr.push(rnd.u8())
            end
            arr
          end)
        else
          "str" + rnd.u32().string()
        end
    end)

primitive \nodoc\ _RowsGen
  fun apply(): Generator[Rows] =>
    Generator[Rows](object is GenObj[Rows]
      fun generate(rnd: Randomness): Rows =>
        let size = rnd.usize(0, 3)
        let rows = recover iso Array[Row](size) end
        for i in Range(0, size) do
          let field_count = rnd.usize(0, 5)
          let fields = recover iso Array[Field](field_count) end
          for j in Range(0, field_count) do
            fields.push(Field("f" + j.string(),
              _random_field_value(rnd)))
          end
          rows.push(Row(consume fields))
        end
        Rows(consume rows)

      fun _random_field_value(rnd: Randomness): FieldData =>
        match rnd.usize(0, 13)
        | 0 => rnd.bool()
        | 1 => F32.from[I32](rnd.i32())
        | 2 => F64.from[I64](rnd.i64())
        | 3 => rnd.i16()
        | 4 => rnd.i32()
        | 5 => rnd.i64()
        | 6 => None
        | 7 =>
          Bytea(recover val
            let arr = Array[U8](rnd.usize(0, 10))
            for _ in Range(0, arr.space()) do
              arr.push(rnd.u8())
            end
            arr
          end)
        | 8 => PgDate(rnd.i32())
        | 9 => PgInterval(rnd.i64(), rnd.i32(), rnd.i32())
        | 10 =>
            try
              PgTime(MakePgTimeMicroseconds(
                (rnd.i64().abs() % 86_400_000_000).i64())
                as PgTimeMicroseconds)
            else _Unreachable(); I64(0)
            end
        | 11 => PgTimestamp(rnd.i64())
        | 12 =>
          RawBytes(recover val
            let arr = Array[U8](rnd.usize(0, 10))
            for _ in Range(0, arr.space()) do
              arr.push(rnd.u8())
            end
            arr
          end)
        else
          "str" + rnd.u32().string()
        end
    end)

// -- Property Tests --

class \nodoc\ iso _TestFieldReflexiveProperty is Property1[Field]
  fun name(): String => "Field/Equality/Reflexive/Property"

  fun gen(): Generator[Field] =>
    _FieldGen()

  fun ref property(arg1: Field, h: PropertyHelper) =>
    h.assert_true(arg1 == arg1)

class \nodoc\ iso _TestFieldStructuralProperty is Property1[FieldData]
  fun name(): String => "Field/Equality/Structural/Property"

  fun gen(): Generator[FieldData] =>
    _FieldDataGen()

  fun ref property(arg1: FieldData, h: PropertyHelper) =>
    h.assert_true(Field("x", arg1) == Field("x", arg1))

class \nodoc\ iso _TestFieldSymmetricProperty
  is Property2[FieldData, FieldData]
  fun name(): String => "Field/Equality/Symmetric/Property"

  fun gen1(): Generator[FieldData] =>
    _FieldDataGen()

  fun gen2(): Generator[FieldData] =>
    _FieldDataGen()

  fun ref property2(arg1: FieldData, arg2: FieldData,
    h: PropertyHelper)
  =>
    let f1 = Field("x", arg1)
    let f2 = Field("x", arg2)
    h.assert_true(f1.eq(f2) == f2.eq(f1))

class \nodoc\ iso _TestRowReflexiveProperty is Property1[Row]
  fun name(): String => "Row/Equality/Reflexive/Property"

  fun gen(): Generator[Row] =>
    _RowGen()

  fun ref property(arg1: Row, h: PropertyHelper) =>
    h.assert_true(arg1 == arg1)

class \nodoc\ iso _TestRowsReflexiveProperty is Property1[Rows]
  fun name(): String => "Rows/Equality/Reflexive/Property"

  fun gen(): Generator[Rows] =>
    _RowsGen()

  fun ref property(arg1: Rows, h: PropertyHelper) =>
    h.assert_true(arg1 == arg1)

class \nodoc\ iso _TestFieldCustomEqualityReflexiveProperty
  is Property1[(I64, I64)]
  """
  Fields containing custom FieldDataEquatable values are reflexively equal.
  Uses generated I64 pairs converted to F64 to construct _TestPoint values.
  """
  fun name(): String => "Field/Equality/CustomReflexive/Property"

  fun gen(): Generator[(I64, I64)] =>
    Generators.zip2[I64, I64](
      Generators.i64(),
      Generators.i64())

  fun ref property(arg1: (I64, I64), h: PropertyHelper) =>
    let p = _TestPoint(F64.from[I64](arg1._1), F64.from[I64](arg1._2))
    h.assert_true(Field("pt", p) == Field("pt", p))
