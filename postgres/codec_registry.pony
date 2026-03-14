use "collections"

class val CodecRegistry
  """
  Maps PostgreSQL type OIDs to codecs. Immutable — adding a codec or array
  type produces a new registry.

  The default constructor creates a registry with all built-in text and binary
  codecs. Use `with_codec` to register custom codecs and `with_array_type` to
  register custom array type mappings:

  ```pony
  let registry = CodecRegistry
    .with_codec(600, PointBinaryCodec)
    .with_array_type(1017, 600)
  let session = Session(server_info, db_info, notify where registry = registry)
  ```
  """
  let _text_codecs: Map[U32, Codec] val
  let _binary_codecs: Map[U32, Codec] val
  let _custom_array_element_oids: Map[U32, U32] val

  new val create() =>
    """
    Registry with all built-in text and binary codecs.
    """
    _text_codecs = recover val
      let m = Map[U32, Codec]
      // Original 7
      m(16) = _BoolTextCodec
      m(17) = _ByteaTextCodec
      m(20) = _Int8TextCodec
      m(21) = _Int2TextCodec
      m(23) = _Int4TextCodec
      m(700) = _Float4TextCodec
      m(701) = _Float8TextCodec
      // Text passthrough (text-like types)
      m(18) = _TextPassthroughTextCodec   // char
      m(19) = _TextPassthroughTextCodec   // name
      m(25) = _TextPassthroughTextCodec   // text
      m(114) = _TextPassthroughTextCodec  // json
      m(142) = _TextPassthroughTextCodec  // xml
      m(1042) = _TextPassthroughTextCodec // bpchar
      m(1043) = _TextPassthroughTextCodec // varchar
      // String-producing types
      m(26) = _OidTextCodec
      m(1700) = _NumericTextCodec
      m(2950) = _UuidTextCodec
      m(3802) = _JsonbTextCodec
      // Temporal types
      m(1082) = _DateTextCodec
      m(1083) = _TimeTextCodec
      m(1114) = _TimestampTextCodec
      m(1184) = _TimestamptzTextCodec
      m(1186) = _IntervalTextCodec
      m
    end
    _binary_codecs = recover val
      let m = Map[U32, Codec]
      // Original 7
      m(16) = _BoolBinaryCodec
      m(17) = _ByteaBinaryCodec
      m(20) = _Int8BinaryCodec
      m(21) = _Int2BinaryCodec
      m(23) = _Int4BinaryCodec
      m(700) = _Float4BinaryCodec
      m(701) = _Float8BinaryCodec
      // Text passthrough (text-like types)
      m(18) = _TextPassthroughBinaryCodec   // char
      m(19) = _TextPassthroughBinaryCodec   // name
      m(25) = _TextPassthroughBinaryCodec   // text
      m(114) = _TextPassthroughBinaryCodec  // json
      m(142) = _TextPassthroughBinaryCodec  // xml
      m(1042) = _TextPassthroughBinaryCodec // bpchar
      m(1043) = _TextPassthroughBinaryCodec // varchar
      // String-producing types
      m(26) = _OidBinaryCodec
      m(1700) = _NumericBinaryCodec
      m(2950) = _UuidBinaryCodec
      m(3802) = _JsonbBinaryCodec
      // Temporal types
      m(1082) = _DateBinaryCodec
      m(1083) = _TimeBinaryCodec
      m(1114) = _TimestampBinaryCodec
      m(1184) = _TimestampBinaryCodec     // timestamptz same encoding
      m(1186) = _IntervalBinaryCodec
      m
    end
    _custom_array_element_oids = recover val Map[U32, U32] end

  fun val with_codec(oid: U32, codec: Codec): CodecRegistry =>
    """
    Returns a new registry with the given codec added or replacing an existing
    one for the given OID. Supports chaining:
    `CodecRegistry.with_codec(600, A).with_codec(790, B)`.
    """
    CodecRegistry._with_codec(this, oid, codec)

  new val _with_codec(base: CodecRegistry, oid: U32, codec: Codec) =>
    """
    New registry that adds or replaces the codec for the given OID.
    """
    _custom_array_element_oids = base._custom_array_element_oids
    let fmt = codec.format()
    if fmt == 0 then
      _text_codecs = recover val
        let m = Map[U32, Codec]
        for (k, v) in base._text_codecs.pairs() do
          m(k) = v
        end
        m(oid) = codec
        m
      end
      _binary_codecs = base._binary_codecs
    else
      _text_codecs = base._text_codecs
      _binary_codecs = recover val
        let m = Map[U32, Codec]
        for (k, v) in base._binary_codecs.pairs() do
          m(k) = v
        end
        m(oid) = codec
        m
      end
    end

  fun val with_array_type(array_oid: U32, element_oid: U32): CodecRegistry =>
    """
    Returns a new registry with a custom array type mapping. This enables
    decode of arrays whose element type is a custom codec-registered OID.
    Supports chaining with `with_codec`:
    `CodecRegistry.with_codec(600, PointCodec).with_array_type(1017, 600)`.
    """
    CodecRegistry._with_array_type(this, array_oid, element_oid)

  new val _with_array_type(base: CodecRegistry, array_oid: U32,
    element_oid: U32)
  =>
    _text_codecs = base._text_codecs
    _binary_codecs = base._binary_codecs
    _custom_array_element_oids = recover val
      let m = Map[U32, U32]
      for (k, v) in base._custom_array_element_oids.pairs() do
        m(k) = v
      end
      m(array_oid) = element_oid
      m
    end

  fun array_oid_for(element_oid: U32): U32 =>
    """
    Return the array OID for a given element OID. Checks built-in mappings
    first, then custom mappings. Returns 0 if no mapping exists.
    """
    try
      _ArrayOidMap.array_oid_for(element_oid)?
    else
      // Check custom mappings (reverse lookup)
      for (arr_oid, elem_oid) in _custom_array_element_oids.pairs() do
        if elem_oid == element_oid then
          return arr_oid
        end
      end
      U32(0)
    end

  fun decode(oid: U32, format: U16, data: Array[U8] val): FieldData =>
    """
    Decode result column data using the registered codec. Array OIDs are
    intercepted and decoded as `PgArray` before falling through to per-OID
    codec lookup.

    Format 0 uses the text codec, format 1 uses the binary codec.
    Text fallback for unknown OIDs: `String.from_array(data)`.
    Binary fallback for unknown OIDs: `RawBytes(data)`.
    """
    // Check built-in array OIDs
    if _ArrayOidMap.is_array_oid(oid) then
      try
        if format == 0 then
          return _decode_text_array(oid, data)?
        else
          return _decode_binary_array(data)?
        end
      else
        // Malformed array data falls through to fallback
        if format == 0 then
          return String.from_array(data)
        else
          return RawBytes(data)
        end
      end
    end

    // Check custom array OIDs
    if _custom_array_element_oids.contains(oid) then
      try
        if format == 0 then
          return _decode_text_array(oid, data)?
        else
          return _decode_binary_array(data)?
        end
      else
        if format == 0 then
          return String.from_array(data)
        else
          return RawBytes(data)
        end
      end
    end

    if format == 0 then
      try
        _text_codecs(oid)?.decode(data)?
      else
        String.from_array(data)
      end
    else
      try
        _binary_codecs(oid)?.decode(data)?
      else
        RawBytes(data)
      end
    end

  fun has_binary_codec(oid: U32): Bool =>
    """
    Whether a binary codec is registered for this OID. Returns true for
    known array OIDs (built-in and custom) in addition to scalar codecs.
    """
    _binary_codecs.contains(oid)
      or _ArrayOidMap.is_array_oid(oid)
      or _custom_array_element_oids.contains(oid)

  fun _decode_binary_array(data: Array[U8] val): PgArray ? =>
    """
    Decode binary array wire format into PgArray.
    """
    if data.size() < 12 then error end
    let ndim = ifdef bigendian then
      data.read_u32(0)?.i32()
    else
      data.read_u32(0)?.bswap().i32()
    end
    if ndim < 0 then error end
    if ndim > 1 then error end

    let has_null = ifdef bigendian then
      data.read_u32(4)?.i32()
    else
      data.read_u32(4)?.bswap().i32()
    end
    if (has_null != 0) and (has_null != 1) then error end

    let element_oid = ifdef bigendian then
      data.read_u32(8)?
    else
      data.read_u32(8)?.bswap()
    end

    if ndim == 0 then
      return PgArray(element_oid,
        recover val Array[(FieldData | None)] end)
    end

    if data.size() < 20 then error end
    let dim_size = ifdef bigendian then
      data.read_u32(12)?.i32()
    else
      data.read_u32(12)?.bswap().i32()
    end
    if dim_size < 0 then error end
    // Skip lower_bound at offset 16

    // Validate minimum data size: dim_size elements * 4 bytes minimum
    let min_element_bytes =
      dim_size.usize().mul_partial(4)?
    if (20 + min_element_bytes) > data.size() then error end

    let elements = recover iso
      let elems = Array[(FieldData | None)](dim_size.usize())
      var offset: USize = 20
      var i: I32 = 0
      while i < dim_size do
        if (offset + 4) > data.size() then error end
        let elem_len = ifdef bigendian then
          data.read_u32(offset)?.i32()
        else
          data.read_u32(offset)?.bswap().i32()
        end
        offset = offset + 4
        if elem_len == -1 then
          elems.push(None)
        elseif elem_len < 0 then
          error
        else
          let len = elem_len.usize()
          if (offset + len) > data.size() then error end
          let elem_data: Array[U8] val = recover val data.trim(offset, offset + len) end
          elems.push(decode(element_oid, 1, elem_data))
          offset = offset + len
        end
        i = i + 1
      end
      if offset != data.size() then error end
      elems
    end
    PgArray(element_oid, consume elements)

  fun _decode_text_array(array_oid: U32, data: Array[U8] val): PgArray ? =>
    """
    Decode text array format into PgArray. Handles simple elements, quoted
    elements with backslash escaping, NULL, and empty arrays. Rejects
    multi-dimensional arrays.
    """
    let s: String val = String.from_array(data)
    if s.size() < 2 then error end
    if s(0)? != '{' then error end
    if s(s.size() - 1)? != '}' then error end

    let element_oid: U32 = try
      _ArrayOidMap.element_oid_for(array_oid)?
    else
      try
        _custom_array_element_oids(array_oid)?
      else
        error
      end
    end

    // Empty array
    if s.size() == 2 then
      return PgArray(element_oid,
        recover val Array[(FieldData | None)] end)
    end

    // Check for multi-dimensional array
    if s(1)? == '{' then error end

    let elements: Array[(FieldData | None)] val = recover val
      let elems = Array[(FieldData | None)]
      var pos: USize = 1  // skip opening '{'
      let end_pos = s.size() - 1  // before closing '}'

      while pos < end_pos do
        if s(pos)? == '"' then
          // Quoted element — use iso array for byte accumulation
          pos = pos + 1
          var buf = recover iso Array[U8] end
          while pos < end_pos do
            let ch = s(pos)?
            if ch == '\\' then
              pos = pos + 1
              if pos >= end_pos then error end
              buf.push(s(pos)?)
            elseif ch == '"' then
              break
            else
              buf.push(ch)
            end
            pos = pos + 1
          end
          if pos >= end_pos then error end
          pos = pos + 1  // skip closing '"'
          let raw: Array[U8] val = consume buf
          elems.push(decode(element_oid, 0, raw))
        else
          // Unquoted element — read until ',' or end
          let start = pos
          while (pos < end_pos) and (s(pos)? != ',') do
            pos = pos + 1
          end
          let token: String val = s.substring(start.isize(), pos.isize())
          // Case-insensitive NULL check
          if (token.size() == 4)
            and (((token(0)? == 'N') or (token(0)? == 'n'))
            and ((token(1)? == 'U') or (token(1)? == 'u'))
            and ((token(2)? == 'L') or (token(2)? == 'l'))
            and ((token(3)? == 'L') or (token(3)? == 'l')))
          then
            elems.push(None)
          else
            let raw: Array[U8] val = token.array()
            elems.push(decode(element_oid, 0, raw))
          end
        end

        // Skip comma separator
        if (pos < end_pos) and (s(pos)? == ',') then
          pos = pos + 1
        end
      end
      elems
    end
    PgArray(element_oid, elements)
