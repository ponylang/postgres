## Fix crash when closing a Session before connection initialization completes

Closing a `Session` immediately after creating it could crash if the close message arrived before the underlying connection actor finished its internal initialization. This was a race condition between Pony's causal messaging guarantees — the initialization message (self-to-self) and the close message (external sender) have no ordering guarantee. The race was unlikely but was observed on macOS arm64.
