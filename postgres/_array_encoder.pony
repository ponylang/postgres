primitive _ArrayEncoder
  """
  Encodes `PgArray` to PostgreSQL binary array wire format.

  Binary array format:
  ```
  I32  ndim            (1 for 1-D, 0 for empty)
  I32  has_null        (0 or 1)
  I32  element_oid
  I32  dimension_size  (number of elements)
  I32  lower_bound     (1)
  Per element:
    I32  element_length (-1 = NULL, else byte count)
    Byte[length] data
  ```

  Coupling: element encoding must stay in sync with
  `_FrontendMessage.bind()` and `_binary_codecs.pony`. Changes to scalar
  binary encoding in those files must be mirrored here.
  """
  fun apply(a: PgArray): Array[U8] val ? =>
    if a.elements.size() == 0 then
      return recover val
        let msg = Array[U8].init(0, 12)
        // ndim=0, has_null=0, element_oid
        ifdef bigendian then
          msg.update_u32(8, a.element_oid)?
        else
          msg.update_u32(8, a.element_oid.bswap())?
        end
        msg
      end
    end

    // Encode each element first
    let encoded: Array[(Array[U8] val | None)] val = recover val
      let enc = Array[(Array[U8] val | None)](a.elements.size())
      for elem in a.elements.values() do
        match elem
        | None => enc.push(None)
        | let fd: FieldData => enc.push(_encode_element(fd, a.element_oid)?)
        end
      end
      enc
    end

    var has_null: U32 = 0
    // Calculate total size
    // 20 bytes header + per-element (4 bytes length + data)
    var data_size: USize = 20
    for e in encoded.values() do
      data_size = data_size + 4
      match e
      | None => has_null = 1
      | let bytes: Array[U8] val => data_size = data_size + bytes.size()
      end
    end

    recover val
      let msg = Array[U8].init(0, data_size)
      ifdef bigendian then
        msg.update_u32(0, U32(1))?         // ndim
        msg.update_u32(4, has_null)?        // has_null
        msg.update_u32(8, a.element_oid)?   // element_oid
        msg.update_u32(12, a.elements.size().u32())? // dimension_size
        msg.update_u32(16, U32(1))?         // lower_bound
      else
        msg.update_u32(0, U32(1).bswap())?
        msg.update_u32(4, has_null.bswap())?
        msg.update_u32(8, a.element_oid.bswap())?
        msg.update_u32(12, a.elements.size().u32().bswap())?
        msg.update_u32(16, U32(1).bswap())?
      end

      var offset: USize = 20
      for e in encoded.values() do
        match e
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
      end
      msg
    end

  fun _encode_element(fd: FieldData, element_oid: U32): Array[U8] val ? =>
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
    | let v: String =>
      // Route by element_oid for string-producing types
      match element_oid
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
