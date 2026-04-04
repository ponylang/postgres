use "collections"

class val CodecRegistry
  """
  Maps PostgreSQL type OIDs to codecs. Immutable — adding a codec, enum,
  composite, or array type produces a new registry.

  The default constructor creates a registry with all built-in text and binary
  codecs. Use `with_codec` to register custom codecs, `with_enum_type` to
  register user-defined enum types, `with_composite_type` to register
  user-defined composite types, and `with_array_type` to register custom
  array type mappings:

  ```pony
  let registry = CodecRegistry
    .with_codec(600, PointBinaryCodec)?       // custom point type
    .with_enum_type(12345)?                   // mood enum
    .with_composite_type(16400,               // address composite
      recover val
        [as (String, U32): ("street", 25); ("city", 25); ("zip_code", 23)]
      end)?
    .with_array_type(12350, 12345)?           // mood[]
    .with_array_type(16401, 16400)?           // address[]
  let session = Session(server_info, db_info, notify where registry = registry)
  ```
  """
  let _text_codecs: Map[U32, Codec] val
  let _binary_codecs: Map[U32, Codec] val
  let _custom_array_element_oids: Map[U32, U32] val
  let _composite_field_descriptors:
    Map[U32, (Array[String] val, Array[U32] val)] val

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
    _composite_field_descriptors =
      recover val Map[U32, (Array[String] val, Array[U32] val)] end

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
    if _composite_field_descriptors.contains(oid) then error end
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
    _composite_field_descriptors = base._composite_field_descriptors
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

  fun val with_enum_type(oid: U32): CodecRegistry ? =>
    """
    Returns a new registry with text-passthrough codecs registered for the
    given enum OID in both text and binary formats. PostgreSQL enum types use
    raw UTF-8 labels on the wire in both formats, so the driver decodes them
    as `String`. Only use this for enum type OIDs — other dynamically-assigned
    types (composites, ranges) have different binary wire formats and would
    produce garbage `String` values.

    Supports chaining:
    `CodecRegistry.with_enum_type(12345)?.with_enum_type(12346)?`.

    Composes with `with_array_type` for enum arrays:
    `CodecRegistry.with_enum_type(12345)?.with_array_type(12350, 12345)?`.

    Errors if the OID is already registered (built-in or custom) or collides
    with a built-in or custom array OID — same validation semantics as
    `with_codec`.

    Because this registers the OID in both codec maps atomically, a subsequent
    `with_codec(oid, ...)` will error for either format. This is stricter than
    two separate `with_codec` calls (which allow independent text/binary
    registration for the same OID).
    """
    if _ArrayOidMap.is_array_oid(oid) then error end
    if _custom_array_element_oids.contains(oid) then error end
    if _composite_field_descriptors.contains(oid) then error end
    if _text_codecs.contains(oid) then error end
    if _binary_codecs.contains(oid) then error end
    CodecRegistry._with_enum_type(this, oid)

  new val _with_enum_type(base: CodecRegistry, oid: U32) =>
    """
    New registry that adds text-passthrough codecs for the given enum OID in
    both text and binary formats.
    """
    _custom_array_element_oids = base._custom_array_element_oids
    _composite_field_descriptors = base._composite_field_descriptors
    _text_codecs = recover val
      let m = Map[U32, Codec]
      for (k, v) in base._text_codecs.pairs() do
        m(k) = v
      end
      m(oid) = _TextPassthroughTextCodec
      m
    end
    _binary_codecs = recover val
      let m = Map[U32, Codec]
      for (k, v) in base._binary_codecs.pairs() do
        m(k) = v
      end
      m(oid) = _TextPassthroughBinaryCodec
      m
    end

  fun val with_composite_type(oid: U32,
    field_descriptors: Array[(String, U32)] val): CodecRegistry ?
  =>
    """
    Returns a new registry with the given composite type registered.
    `field_descriptors` are `(name, oid)` pairs in declaration order.

    Supports chaining:
    `CodecRegistry.with_composite_type(16400, fields)?.with_array_type(16401, 16400)?`.

    Errors if the OID is already registered (built-in, custom codec, enum,
    or composite), collides with a built-in or custom array OID, or has
    empty field descriptors. Self-referential field OIDs (the composite's
    own OID in its field list) are also rejected.

    Does NOT auto-register the corresponding array type — call
    `with_array_type` separately if needed.
    """
    if field_descriptors.size() == 0 then error end
    if _ArrayOidMap.is_array_oid(oid) then error end
    if _custom_array_element_oids.contains(oid) then error end
    if _text_codecs.contains(oid) then error end
    if _binary_codecs.contains(oid) then error end
    if _composite_field_descriptors.contains(oid) then error end
    // Reject self-referential field OIDs
    for (_, field_oid) in field_descriptors.values() do
      if field_oid == oid then error end
    end
    let names: Array[String] val = recover val
      let n = Array[String](field_descriptors.size())
      for (name, _) in field_descriptors.values() do
        n.push(name)
      end
      n
    end
    let oids: Array[U32] val = recover val
      let o = Array[U32](field_descriptors.size())
      for (_, field_oid) in field_descriptors.values() do
        o.push(field_oid)
      end
      o
    end
    CodecRegistry._with_composite_type(this, oid, names, oids)

  new val _with_composite_type(base: CodecRegistry, oid: U32,
    names: Array[String] val, oids: Array[U32] val)
  =>
    """
    New registry that adds the composite type mapping for the given OID.
    """
    _text_codecs = base._text_codecs
    _binary_codecs = base._binary_codecs
    _custom_array_element_oids = base._custom_array_element_oids
    _composite_field_descriptors = recover val
      let m = Map[U32, (Array[String] val, Array[U32] val)]
      for (k, v) in base._composite_field_descriptors.pairs() do
        m(k) = v
      end
      m(oid) = (names, oids)
      m
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

    // Reject array OIDs that collide with composite OIDs
    if _composite_field_descriptors.contains(array_oid) then error end

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
    _composite_field_descriptors = base._composite_field_descriptors
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
    Decode result column data using the registered codec. Array and composite
    OIDs are intercepted before falling through to per-OID codec lookup.

    Format 0 uses the text codec, format 1 uses the binary codec.

    If no codec is registered for the OID, returns a fallback value:
    `String.from_array(data)` for text format, `RawBytes(data)` for binary.

    If a codec IS registered but its `decode()` errors, the error propagates
    to the caller. This surfaces malformed data from the server (built-in
    codecs) and broken custom codecs instead of silently returning fallback
    values.

    For arrays and composites, structural parsing errors (malformed wire
    format) fall back, but element/field codec errors propagate.
    """
    _decode_with_depth(oid, format, data, 0)?

  fun _decode_with_depth(oid: U32, format: U16, data: Array[U8] val,
    depth: USize): FieldData ?
  =>
    """
    Depth-aware decode implementation. `depth` tracks recursion through
    nested arrays and composites to prevent stack overflow from pathological
    schemas.
    """
    if depth > _max_decode_depth() then error end

    // Check built-in array OIDs
    if _ArrayOidMap.is_array_oid(oid) then
      let parsed = try
        if format == 0 then _parse_text_array(oid, data)?
        else _parse_binary_array(data)? end
      else
        if format == 0 then return String.from_array(data)
        else return RawBytes(data) end
      end
      return _decode_array_elements(parsed._1, format, parsed._2, depth)?
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
      return _decode_array_elements(parsed._1, format, parsed._2, depth)?
    end

    // Check composite OIDs — no fallback; parsing errors propagate because
    // composites are explicitly registered (unlike arrays, which might be
    // unrecognized formats). Field count mismatches indicate schema drift.
    if _composite_field_descriptors.contains(oid) then
      let parsed = if format == 0 then _parse_text_composite(oid, data)?
        else _parse_binary_composite(data)? end
      return _decode_composite_fields(oid, format, parsed, depth)?
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

  fun _max_decode_depth(): USize => 16

  fun has_binary_codec(oid: U32): Bool =>
    """
    Whether a binary codec is registered for this OID. Returns true for
    known array OIDs (built-in and custom) and composite OIDs in addition
    to scalar codecs.
    """
    _binary_codecs.contains(oid)
      or _ArrayOidMap.is_array_oid(oid)
      or _custom_array_element_oids.contains(oid)
      or _composite_field_descriptors.contains(oid)

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
    raw_elements: Array[(Array[U8] val | None)] val, depth: USize): PgArray ?
  =>
    """
    Decode raw element byte arrays into typed `FieldData` values using the
    registered codec for the element OID. Errors from element codec `decode()`
    propagate to the caller.

    Rejects array OIDs as `element_oid` to prevent recursive decoding. The
    binary path extracts `element_oid` from untrusted wire data, so a malicious
    server could embed an array OID to cause `decode` to re-enter array
    decoding. The text path is not affected (it looks up `element_oid` from the
    registry), but the guard applies uniformly.
    """
    if _ArrayOidMap.is_array_oid(element_oid) then error end
    if _custom_array_element_oids.contains(element_oid) then error end
    let elements = recover iso
      let elems = Array[(FieldData | None)](raw_elements.size())
      for raw in raw_elements.values() do
        match raw
        | None => elems.push(None)
        | let bytes: Array[U8] val =>
          elems.push(_decode_with_depth(element_oid, format, bytes,
            depth + 1)?)
        end
      end
      elems
    end
    PgArray(element_oid, consume elements)

  fun _parse_binary_composite(data: Array[U8] val)
    : Array[(U32, (Array[U8] val | None))] val ?
  =>
    """
    Parse binary composite wire format, extracting per-field OIDs and raw
    byte slices without decoding them.

    Binary composite format:
    ```
    I32  field_count
    Per field:
      I32  field_oid
      I32  field_len (-1 = NULL)
      Byte[len] data
    ```
    """
    if data.size() < 4 then error end
    let field_count = ifdef bigendian then
      data.read_u32(0)?.i32()
    else
      data.read_u32(0)?.bswap().i32()
    end
    if field_count < 0 then error end

    // Validate minimum data size: each field requires at least 8 bytes
    // (4 OID + 4 length). Prevents allocation from untrusted field_count.
    let min_field_bytes = field_count.usize().mul_partial(8)?
    if (4 + min_field_bytes) > data.size() then error end

    recover val
      let fields = Array[(U32, (Array[U8] val | None))](field_count.usize())
      var offset: USize = 4
      var i: I32 = 0
      while i < field_count do
        if (offset + 8) > data.size() then error end
        let field_oid = ifdef bigendian then
          data.read_u32(offset)?
        else
          data.read_u32(offset)?.bswap()
        end
        offset = offset + 4
        let field_len = ifdef bigendian then
          data.read_u32(offset)?.i32()
        else
          data.read_u32(offset)?.bswap().i32()
        end
        offset = offset + 4
        if field_len == -1 then
          fields.push((field_oid, None))
        elseif field_len < 0 then
          error
        else
          let len = field_len.usize()
          if (offset + len) > data.size() then error end
          fields.push((field_oid,
            recover val data.trim(offset, offset + len) end))
          offset = offset + len
        end
        i = i + 1
      end
      if offset != data.size() then error end
      fields
    end

  fun _parse_text_composite(oid: U32, data: Array[U8] val)
    : Array[(U32, (Array[U8] val | None))] val ?
  =>
    """
    Parse text composite format `(val1,val2,val3)`, extracting raw field
    byte arrays without decoding them. Uses registered field descriptors
    for per-position OID lookup.

    Text composite format differs from array text format:
    - Delimiters: `()` not `{}`
    - NULL: empty unquoted position (not the keyword `NULL`)
    - Empty string: `""` (quoted empty)
    - Escaping: doubled `""` and `\\` (not backslash-as-escape like arrays)
    - Nested composites appear as quoted strings with inner quoting
    """
    let s: String val = String.from_array(data)
    if s.size() < 2 then error end
    if s(0)? != '(' then error end
    if s(s.size() - 1)? != ')' then error end

    (_, let field_oids) = _composite_field_descriptors(oid)?

    let raw_fields: Array[(U32, (Array[U8] val | None))] val = recover val
      let fields = Array[(U32, (Array[U8] val | None))](field_oids.size())
      var pos: USize = 1  // skip opening '('
      let end_pos = s.size() - 1  // before closing ')'
      var field_idx: USize = 0

      var after_comma = true  // start of content is like after a delimiter
      while (pos <= end_pos) and (field_idx < field_oids.size()) do
        let field_oid = field_oids(field_idx)?

        if pos == end_pos then
          if after_comma then
            // Trailing NULL — a comma preceded the closing paren
            fields.push((field_oid, None))
            field_idx = field_idx + 1
          end
          // Either way, exit — we're at the end
          break
        elseif s(pos)? == ',' then
          // Empty unquoted position = NULL
          fields.push((field_oid, None))
          pos = pos + 1
          after_comma = true
          field_idx = field_idx + 1
        elseif s(pos)? == '"' then
          // Quoted field — doubled-character escaping for " and \
          pos = pos + 1
          var buf = recover iso Array[U8] end
          while pos < end_pos do
            let ch = s(pos)?
            if ch == '"' then
              // Check for doubled double-quote
              if ((pos + 1) < end_pos) and (s(pos + 1)? == '"') then
                buf.push('"')
                pos = pos + 2
              else
                // End of quoted value
                break
              end
            elseif ch == '\\' then
              // Check for doubled backslash
              if ((pos + 1) < end_pos) and (s(pos + 1)? == '\\') then
                buf.push('\\')
                pos = pos + 2
              else
                buf.push(ch)
                pos = pos + 1
              end
            else
              buf.push(ch)
              pos = pos + 1
            end
          end
          if (pos >= end_pos) and (s(pos)? != '"') then error end
          pos = pos + 1  // skip closing '"'
          fields.push((field_oid, consume buf))
          // Skip comma separator
          after_comma = false
          if (pos < end_pos) and (s(pos)? == ',') then
            pos = pos + 1
            after_comma = true
          end
          field_idx = field_idx + 1
        else
          // Unquoted value — read until ',' or ')'
          let start = pos
          while (pos < end_pos) and (s(pos)? != ',') do
            pos = pos + 1
          end
          let token: String val = s.substring(start.isize(), pos.isize())
          fields.push((field_oid, token.array()))
          // Skip comma separator
          after_comma = false
          if (pos < end_pos) and (s(pos)? == ',') then
            pos = pos + 1
            after_comma = true
          end
          field_idx = field_idx + 1
        end
      end

      if field_idx != field_oids.size() then error end
      // Verify all input was consumed (extra fields = schema mismatch)
      if pos != end_pos then error end
      fields
    end
    raw_fields

  fun _decode_composite_fields(oid: U32, format: U16,
    parsed: Array[(U32, (Array[U8] val | None))] val, depth: USize)
    : PgComposite ?
  =>
    """
    Decode raw composite field data into a `PgComposite`. For binary format,
    uses wire OIDs for codec selection (authoritative). For text format, uses
    registered OIDs. Errors on field count mismatch with registration.
    """
    (let names, let registered_oids) = _composite_field_descriptors(oid)?

    if parsed.size() != registered_oids.size() then error end

    let wire_oids: Array[U32] val = recover val
      let o = Array[U32](parsed.size())
      for (field_oid, _) in parsed.values() do
        o.push(field_oid)
      end
      o
    end

    let fields = recover iso
      let f = Array[(FieldData | None)](parsed.size())
      for (field_oid, raw) in parsed.values() do
        match raw
        | None => f.push(None)
        | let bytes: Array[U8] val =>
          f.push(_decode_with_depth(field_oid, format, bytes, depth + 1)?)
        end
      end
      f
    end
    PgComposite(oid, wire_oids, names, consume fields)?
