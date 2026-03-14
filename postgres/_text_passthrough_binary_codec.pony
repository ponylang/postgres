primitive _TextPassthroughBinaryCodec is Codec
  """
  Binary codec for PostgreSQL text-like types where the binary wire format is
  raw UTF-8 bytes — identical to the text format. Registered for OIDs: char
  (18), name (19), text (25), json (114), xml (142), bpchar (1042), varchar
  (1043).
  """
  fun format(): U16 => 1

  fun encode(value: FieldDataTypes): Array[U8] val ? =>
    match value
    | let s: String => s.array()
    else
      error
    end

  fun decode(data: Array[U8] val): FieldDataTypes =>
    String.from_array(data)
