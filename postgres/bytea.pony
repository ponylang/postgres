class val Bytea is (FieldData & Equatable[Bytea])
  """
  Wraps `Array[U8] val` for PostgreSQL `bytea` columns (OID 17). Use `.data`
  to access the raw bytes.

  This type appears in result fields where previous versions returned
  `Array[U8] val` directly. The wrapper is needed because `Array[U8] val`
  does not implement `Stringable`, which `FieldData` requires.
  """
  let data: Array[U8] val

  new val create(data': Array[U8] val) =>
    data = data'

  fun eq(that: box->Bytea): Bool =>
    if data.size() != that.data.size() then return false end
    try
      var i: USize = 0
      while i < data.size() do
        if data(i)? != that.data(i)? then return false end
        i = i + 1
      end
      true
    else
      _Unreachable()
      false
    end

  fun string(): String iso^ =>
    """
    Returns a hex representation of the bytes (e.g., `\\xdeadbeef`).
    """
    recover iso
      let s = String(2 + (data.size() * 2))
      s.append("\\x")
      for b in data.values() do
        s.push(_to_hex((b >> 4) and 0x0F))
        s.push(_to_hex(b and 0x0F))
      end
      s
    end

  fun tag _to_hex(nibble: U8): U8 =>
    if nibble < 10 then nibble + '0'
    else (nibble - 10) + 'a'
    end
