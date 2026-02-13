class val Field is Equatable[Field]
  let name: String
  let value: FieldDataTypes

  new val create(name': String, value': FieldDataTypes) =>
    name = name'
    value = value'

  fun eq(that: box->Field): Bool =>
    """
    Two fields are equal when they have the same name and the same value.
    Values must be the same type and compare equal using the type's own
    equality.
    """
    if name != that.name then return false end
    match (value, that.value)
    | (let a: Array[U8] val, let b: Array[U8] val) =>
      if a.size() != b.size() then return false end
      try
        var i: USize = 0
        while i < a.size() do
          if a(i)? != b(i)? then return false end
          i = i + 1
        end
        true
      else
        false
      end
    | (let a: Bool, let b: Bool) => a == b
    | (let a: F32, let b: F32) => a == b
    | (let a: F64, let b: F64) => a == b
    | (let a: I16, let b: I16) => a == b
    | (let a: I32, let b: I32) => a == b
    | (let a: I64, let b: I64) => a == b
    | (None, None) => true
    | (let a: String, let b: String) => a == b
    else
      false
    end
