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
    .with_codec(600, PointBinaryCodec)?
    .with_array_type(1017, 600)?
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

  fun val with_codec(oid: U32, codec: Codec): CodecRegistry ? =>
    """
    Returns a new registry with the given codec registered for the given OID.
    Supports chaining:
    `CodecRegistry.with_codec(600, A)?.with_codec(790, B)?`.

    Errors if the OID is already registered (built-in or custom) or collides
    with a built-in or custom array OID. Use distinct OIDs for each codec.
    """
    if _ArrayOidMap.is_array_oid(oid) then error end
    if _custom_array_element_oids.contains(oid) then error end
    if codec.format() == 0 then
      if _text_codecs.contains(oid) then error end
    else
      if _binary_codecs.contains(oid) then error end
    end
    CodecRegistry._with_codec(this, oid, codec)

  new val _with_codec(base: CodecRegistry, oid: U32, codec: Codec) =>
    """
    New registry that adds the codec for the given OID.
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

  fun val with_array_type(array_oid: U32, element_oid: U32)
    : CodecRegistry ?
  =>
    """
    Returns a new registry with a custom array type mapping. This enables
    decode of arrays whose element type is a custom codec-registered OID.
    Supports chaining with `with_codec`:
    `CodecRegistry.with_codec(600, PointCodec)?.with_array_type(1017, 600)?`.

    Errors if:
    - `element_oid` is itself an array OID (built-in or custom), which would
      cause unbounded recursion during decode
    - `array_oid` collides with a registered scalar or built-in array OID
    - `array_oid` is already registered as a custom array OID
    - `array_oid` is already registered as a custom element OID
    - `array_oid == element_oid`
    """
    // Reject self-referential mapping
    if array_oid == element_oid then error end

    // Reject element OIDs that are themselves array OIDs (recursion)
    if _ArrayOidMap.is_array_oid(element_oid) then error end
    if _custom_array_element_oids.contains(element_oid) then error end

    // Reject array OIDs that collide with registered scalar codecs
    if _text_codecs.contains(array_oid) then error end
    if _binary_codecs.contains(array_oid) then error end

    // Reject array OIDs that collide with built-in array OIDs
    if _ArrayOidMap.is_array_oid(array_oid) then error end

    // Reject duplicate custom array OID registrations
    if _custom_array_element_oids.contains(array_oid) then error end

    // Reject array OIDs that are already registered as custom element OIDs
    for elem_oid in _custom_array_element_oids.values() do
      if elem_oid == array_oid then error end
    end

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

  fun decode(oid: U32, format: U16, data: Array[U8] val): FieldData ? =>
    """
    Decode result column data using the registered codec. Array OIDs are
    intercepted and decoded as `PgArray` before falling through to per-OID
    codec lookup.

    Format 0 uses the text codec, format 1 uses the binary codec.

    If no codec is registered for the OID, returns a fallback value:
    `String.from_array(data)` for text format, `RawBytes(data)` for binary.

    If a codec IS registered but its `decode()` errors, the error propagates
    to the caller. This surfaces malformed data from the server (built-in
    codecs) and broken custom codecs instead of silently returning fallback
    values.

    For arrays, structural parsing errors (malformed wire format) fall back,
    but element codec errors propagate. This distinguishes between an
    unrecognized array format (which might be a new PostgreSQL feature) and
    corrupt element data (which should be surfaced).
    """
    // Check built-in array OIDs
    if _ArrayOidMap.is_array_oid(oid) then
      let parsed = try
        if format == 0 then _parse_text_array(oid, data)?
        else _parse_binary_array(data)? end
      else
        if format == 0 then return String.from_array(data)
        else return RawBytes(data) end
      end
      return _decode_array_elements(parsed._1, format, parsed._2)?
    end

    // Check custom array OIDs
    if _custom_array_element_oids.contains(oid) then
      let parsed = try
        if format == 0 then _parse_text_array(oid, data)?
        else _parse_binary_array(data)? end
      else
        if format == 0 then return String.from_array(data)
        else return RawBytes(data) end
      end
      return _decode_array_elements(parsed._1, format, parsed._2)?
    end

    if format == 0 then
      let codec = try
        _text_codecs(oid)?
      else
        return String.from_array(data)
      end
      codec.decode(data)?
    else
      let codec = try
        _binary_codecs(oid)?
      else
        return RawBytes(data)
      end
      codec.decode(data)?
    end

  fun has_binary_codec(oid: U32): Bool =>
    """
    Whether a binary codec is registered for this OID. Returns true for
    known array OIDs (built-in and custom) in addition to scalar codecs.
    """
    _binary_codecs.contains(oid)
      or _ArrayOidMap.is_array_oid(oid)
      or _custom_array_element_oids.contains(oid)

  fun _parse_binary_array(data: Array[U8] val)
    : (U32, Array[(Array[U8] val | None)] val) ?
  =>
    """
    Parse binary array wire format, extracting the element OID and raw element
    byte slices without decoding them. Structural validation errors (truncated
    data, multi-dimensional, bad offsets) raise `error`.
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
      return (element_oid,
        recover val Array[(Array[U8] val | None)] end)
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

    let raw_elements = recover val
      let elems = Array[(Array[U8] val | None)](dim_size.usize())
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
          elems.push(recover val data.trim(offset, offset + len) end)
          offset = offset + len
        end
        i = i + 1
      end
      if offset != data.size() then error end
      elems
    end
    (element_oid, raw_elements)

  fun _parse_text_array(array_oid: U32, data: Array[U8] val)
    : (U32, Array[(Array[U8] val | None)] val) ?
  =>
    """
    Parse text array format, extracting the element OID and raw element byte
    arrays without decoding them. Handles simple elements, quoted elements with
    backslash escaping, NULL, and empty arrays. Rejects multi-dimensional
    arrays.
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
      return (element_oid,
        recover val Array[(Array[U8] val | None)] end)
    end

    // Check for multi-dimensional array
    if s(1)? == '{' then error end

    let raw_elements: Array[(Array[U8] val | None)] val = recover val
      let elems = Array[(Array[U8] val | None)]
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
          elems.push(consume buf)
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
            elems.push(token.array())
          end
        end

        // Skip comma separator
        if (pos < end_pos) and (s(pos)? == ',') then
          pos = pos + 1
        end
      end
      elems
    end
    (element_oid, raw_elements)

  fun _decode_array_elements(element_oid: U32, format: U16,
    raw_elements: Array[(Array[U8] val | None)] val): PgArray ?
  =>
    """
    Decode raw element byte arrays into typed `FieldData` values using the
    registered codec for the element OID. Errors from element codec `decode()`
    propagate to the caller.
    """
    let elements = recover iso
      let elems = Array[(FieldData | None)](raw_elements.size())
      for raw in raw_elements.values() do
        match raw
        | None => elems.push(None)
        | let bytes: Array[U8] val =>
          elems.push(decode(element_oid, format, bytes)?)
        end
      end
      elems
    end
    PgArray(element_oid, consume elements)
