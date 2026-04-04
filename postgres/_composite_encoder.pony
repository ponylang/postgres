primitive _CompositeEncoder
  """
  Encodes `PgComposite` to PostgreSQL binary composite wire format.

  Binary composite format:
  ```
  I32  field_count
  Per field:
    I32  field_oid
    I32  field_len (-1 = NULL)
    Byte[len] data
  ```

  Coupling: element encoding must stay in sync with `_ArrayEncoder`,
  `_FrontendMessage.bind()`, and `_binary_codecs.pony`. Changes to scalar
  binary encoding in those files must be mirrored here.
  """
  fun apply(c: PgComposite, registry: CodecRegistry): Array[U8] val ? =>
    // Encode each field first
    let encoded: Array[(Array[U8] val | None)] val = recover val
      let enc = Array[(Array[U8] val | None)](c.fields.size())
      var i: USize = 0
      while i < c.fields.size() do
        match c.fields(i)?
        | None => enc.push(None)
        | let fd: FieldData =>
          enc.push(_encode_field(fd, c.field_oids(i)?, registry)?)
        end
        i = i + 1
      end
      enc
    end

    // Calculate total size: 4 (field_count) + per-field (4 oid + 4 len + data)
    var data_size: USize = 4
    for e in encoded.values() do
      data_size = data_size + 8 // oid + len
      match e
      | let bytes: Array[U8] val => data_size = data_size + bytes.size()
      end
    end

    recover val
      let msg = Array[U8].init(0, data_size)
      ifdef bigendian then
        msg.update_u32(0, c.fields.size().u32())?
      else
        msg.update_u32(0, c.fields.size().u32().bswap())?
      end

      var offset: USize = 4
      var i: USize = 0
      while i < encoded.size() do
        let field_oid = c.field_oids(i)?
        ifdef bigendian then
          msg.update_u32(offset, field_oid)?
        else
          msg.update_u32(offset, field_oid.bswap())?
        end
        offset = offset + 4

        match encoded(i)?
        | None =>
          // NULL: length = -1
          ifdef bigendian then
            msg.update_u32(offset, U32.max_value())?
          else
            msg.update_u32(offset, U32.max_value().bswap())?
          end
          offset = offset + 4
        | let bytes: Array[U8] val =>
          ifdef bigendian then
            msg.update_u32(offset, bytes.size().u32())?
          else
            msg.update_u32(offset, bytes.size().u32().bswap())?
          end
          offset = offset + 4
          msg.copy_from(bytes, 0, offset, bytes.size())
          offset = offset + bytes.size()
        end
        i = i + 1
      end
      msg
    end

  fun _encode_field(fd: FieldData, field_oid: U32,
    registry: CodecRegistry): Array[U8] val ?
  =>
    match fd
    | let v: I16 => _Int2BinaryCodec.encode(v)?
    | let v: I32 => _Int4BinaryCodec.encode(v)?
    | let v: I64 => _Int8BinaryCodec.encode(v)?
    | let v: F32 => _Float4BinaryCodec.encode(v)?
    | let v: F64 => _Float8BinaryCodec.encode(v)?
    | let v: Bool => _BoolBinaryCodec.encode(v)?
    | let v: Bytea => _ByteaBinaryCodec.encode(v.data)?
    | let v: PgDate => _DateBinaryCodec.encode(v)?
    | let v: PgTime => _TimeBinaryCodec.encode(v)?
    | let v: PgTimestamp => _TimestampBinaryCodec.encode(v)?
    | let v: PgInterval => _IntervalBinaryCodec.encode(v)?
    | let c: PgComposite => _CompositeEncoder(c, registry)?
    | let a: PgArray => _ArrayEncoder(a, registry)?
    | let v: String =>
      // Route by field_oid for string-producing types
      match field_oid
      | 2950 => _UuidBinaryCodec.encode(v)?
      | 3802 => _JsonbBinaryCodec.encode(v)?
      | 26 => _OidBinaryCodec.encode(v)?
      | 1700 => _NumericBinaryCodec.encode(v)?
      else
        // text, char, name, json, xml, bpchar, varchar → raw UTF-8
        v.array()
      end
    else
      error
    end
