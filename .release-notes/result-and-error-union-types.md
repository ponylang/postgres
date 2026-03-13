## Change Result and ClientQueryError from traits to union types

`Result` and `ClientQueryError` are now union types instead of traits, enabling compiler-enforced exhaustive matching via `match \exhaustive\`. Previously, the compiler could not verify that all result or error variants were handled. Now, adding `\exhaustive\` to a match on `Result` or `ClientQueryError` produces a compile error if any variant is missing.

This matches the pattern already used by `AuthenticationFailureReason`, `TransactionStatus`, `Query`, `SSLMode`, and `FieldDataTypes` in this library.

Before:

```pony
be pg_query_result(session: Session, result: Result) =>
  match result
  | let r: ResultSet => // ...
  | let r: RowModifying => // ...
  // SimpleResult silently unhandled — no compiler warning
  end
```

After:

```pony
be pg_query_result(session: Session, result: Result) =>
  match \exhaustive\ result
  | let r: ResultSet => // ...
  | let r: RowModifying => // ...
  | let r: SimpleResult => // ...
  end
```

Existing non-exhaustive matches continue to work without changes. The `query()` method remains callable on all three `Result` members without matching first.
