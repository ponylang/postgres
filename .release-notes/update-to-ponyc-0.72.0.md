## Update to work with ponyc 0.72.0

The lori networking library has been merged into ponyc's stdlib as the `net` package. postgres now uses stdlib `net` directly instead of depending on lori as an external package. You no longer need lori in your `corral.json`. Change `use lori = "lori"` to `use "net"` and drop the `lori.` qualifier from type names.
