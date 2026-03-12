primitive _ParamEncoder
  """
  Derives PostgreSQL type OIDs from `FieldDataTypes` parameter values.
  Used to populate the `param_type_oids` array in Parse messages so the
  server knows the expected types for typed parameters. `String` and `None`
  get OID 0 (server infers the type).
  """
  fun oids_for(params: Array[FieldDataTypes] val): Array[U32] val =>
    recover val
      let oids = Array[U32](params.size())
      for p in params.values() do
        oids.push(match p
        | let _: I16 => U32(21)
        | let _: I32 => U32(23)
        | let _: I64 => U32(20)
        | let _: F32 => U32(700)
        | let _: F64 => U32(701)
        | let _: Bool => U32(16)
        | let _: Array[U8] val => U32(17)
        | let _: String => U32(0)
        | None => U32(0)
        end)
      end
      oids
    end
