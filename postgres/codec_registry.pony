use "collections"

class val CodecRegistry
  """
  Maps PostgreSQL type OIDs to codecs. Immutable — adding a codec produces
  a new registry.

  The default constructor creates a registry with all built-in text and binary
  codecs. Users who need custom codecs can create an extended registry with
  `_with_codec` (type-private in Phase 2; public in Phase 3).
  """
  let _text_codecs: Map[U32, Codec] val
  let _binary_codecs: Map[U32, Codec] val

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

  new val _with_codec(base: CodecRegistry, oid: U32, codec: Codec) =>
    """
    New registry that adds or replaces the codec for the given OID.
    Type-private in Phase 2 (underscore on method name); will be made public
    in Phase 3 by removing the underscore.
    """
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

  fun decode(oid: U32, format: U16, data: Array[U8] val): FieldDataTypes =>
    """
    Decode result column data using the registered codec.
    Format 0 uses the text codec, format 1 uses the binary codec.
    Text fallback for unknown OIDs: `String.from_array(data)`.
    Binary fallback for unknown OIDs: raw `Array[U8] val`.
    """
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
        data
      end
    end

  fun has_binary_codec(oid: U32): Bool =>
    """
    Whether a binary codec is registered for this OID.
    """
    _binary_codecs.contains(oid)
