primitive _ArrayOidMap
  """
  Static bidirectional mapping between element OIDs and array OIDs for all
  built-in types with registered codecs.
  """
  fun element_oid_for(array_oid: U32): U32 ? =>
    """
    Return the element OID for a given array OID.
    """
    match array_oid
    | 199 => 114     // json[]
    | 143 => 142     // xml[]
    | 1000 => 16     // bool[]
    | 1001 => 17     // bytea[]
    | 1002 => 18     // char[]
    | 1003 => 19     // name[]
    | 1005 => 21     // int2[]
    | 1007 => 23     // int4[]
    | 1009 => 25     // text[]
    | 1014 => 1042   // bpchar[]
    | 1015 => 1043   // varchar[]
    | 1016 => 20     // int8[]
    | 1021 => 700    // float4[]
    | 1022 => 701    // float8[]
    | 1028 => 26     // oid[]
    | 1115 => 1114   // timestamp[]
    | 1182 => 1082   // date[]
    | 1183 => 1083   // time[]
    | 1185 => 1184   // timestamptz[]
    | 1187 => 1186   // interval[]
    | 1231 => 1700   // numeric[]
    | 2951 => 2950   // uuid[]
    | 3807 => 3802   // jsonb[]
    else
      error
    end

  fun array_oid_for(element_oid: U32): U32 ? =>
    """
    Return the array OID for a given element OID.
    """
    match element_oid
    | 16 => 1000     // bool
    | 17 => 1001     // bytea
    | 18 => 1002     // char
    | 19 => 1003     // name
    | 20 => 1016     // int8
    | 21 => 1005     // int2
    | 23 => 1007     // int4
    | 25 => 1009     // text
    | 26 => 1028     // oid
    | 114 => 199     // json
    | 142 => 143     // xml
    | 700 => 1021    // float4
    | 701 => 1022    // float8
    | 1042 => 1014   // bpchar
    | 1043 => 1015   // varchar
    | 1082 => 1182   // date
    | 1083 => 1183   // time
    | 1114 => 1115   // timestamp
    | 1184 => 1185   // timestamptz
    | 1186 => 1187   // interval
    | 1700 => 1231   // numeric
    | 2950 => 2951   // uuid
    | 3802 => 3807   // jsonb
    else
      error
    end

  fun is_array_oid(oid: U32): Bool =>
    """
    Whether the given OID is a known built-in array type.
    """
    match oid
    | 143 | 199
    | 1000 | 1001 | 1002 | 1003 | 1005 | 1007 | 1009
    | 1014 | 1015 | 1016 | 1021 | 1022 | 1028
    | 1115 | 1182 | 1183 | 1185 | 1187 | 1231
    | 2951 | 3807 =>
      true
    else
      false
    end
