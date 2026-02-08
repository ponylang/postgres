## Update ponylang/ssl dependency to 1.0.1

We've updated the ponylang/ssl library dependency in this project to 1.0.1.

## Fix typo in SesssionNeverOpened

`SesssionNeverOpened` has been renamed to `SessionNeverOpened`.

Before:

```pony
match error
| SesssionNeverOpened => "session never opened"
end
```

After:

```pony
match error
| SessionNeverOpened => "session never opened"
end
```

## Fix ErrorResponseMessage routine field never being populated

The error response parser incorrectly mapped the `'R'` (Routine) protocol field to `line` instead of `routine` on `ErrorResponseMessage`. The `routine` field was never populated as a result. It now correctly contains the name of the source-code routine that reported the error.

