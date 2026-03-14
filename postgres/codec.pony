interface val Codec
  """
  Encodes and decodes values for a PostgreSQL type in a specific wire format
  (text or binary).

  Each codec handles one format. The OID-to-codec mapping is the registry's
  responsibility, not the codec's — a single codec implementation can serve
  multiple OIDs (e.g., one text passthrough codec for all text-like OIDs).

  Codecs are `val` (immutable, shareable across actors). Built-in codecs are
  primitives (zero allocation, global singletons).
  """

  fun format(): U16
    """
    Wire format: 0 for text, 1 for binary.
    """

  fun encode(value: FieldDataTypes): Array[U8] val ?
    """
    Encode a Pony value to wire format bytes for use as a parameter.
    Errors when the value's type doesn't match what this codec expects.
    """

  fun decode(data: Array[U8] val): FieldData ?
    """
    Decode wire format bytes from a result column to a Pony value.
    Errors when the data is malformed or has an unexpected length.

    The return type is `FieldData` (an open interface) rather than
    `FieldDataTypes` (a closed union) so that custom codecs can return their
    own types. The encode path stays `FieldDataTypes` because parameters must
    map to known PostgreSQL OIDs for the wire protocol.
    """
