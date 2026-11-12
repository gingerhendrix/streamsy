---
packages:
  "@streamsy/core": minor
  "@streamsy/storage": patch
---

## Retry failed projection marks

Local memory and SQL storage can make failed projection marks due immediately while preserving their attempts and sequence numbers. The operation selects a binding or a whole projection and returns the number changed.
