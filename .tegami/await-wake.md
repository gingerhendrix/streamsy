---
packages:
  "@streamsy/core": minor
---

## Wait for a wake row to reach an offset

`Streamsy.awaitWake(Wake, params, { stream, offset })` waits until the source row reaches the requested offset over local or remote storage. An optional fourth argument passes `from` (a saved wake cursor) and follow options. Without `from`, each call reads the wake stream from the start. Only rows after a saved cursor can satisfy that wait. Callers choose their own timeout. Wake row offsets can describe either written or accepted positions; the row schema is unchanged.
