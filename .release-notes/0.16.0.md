## Update to work with ponyc 0.78.0

ponyc 0.78.0 changed the `Stringable` interface. `string()` now returns `String` instead of `String iso^`. We have updated accordingly.

## Change FieldData string() return type from String iso^ to String

The `FieldData` interface's `string()` method now returns `String` instead of `String iso^`, matching the ponyc 0.78.0 `Stringable` change. If you have a custom `FieldData` implementation, update its `string()` return type:

Before:

```pony
class val MyType is FieldData
  fun string(): String iso^ =>
    recover iso String .> append("my value") end
```

After:

```pony
class val MyType is FieldData
  fun string(): String =>
    recover val String .> append("my value") end
```
