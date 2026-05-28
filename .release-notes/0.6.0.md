## Require ponyc 0.64.0 or later

postgres now requires ponyc 0.64.0 or later. The previous minimum was 0.63.1.

This is driven by an update to lori 0.15.0, which requires ponyc 0.64.0 for changes to FFI declaration syntax and the runtime socket API. Older ponyc versions will fail to compile postgres.

