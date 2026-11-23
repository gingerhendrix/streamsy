---
packages:
  "@streamsy/views": patch
---

## Order tied view rows consistently

When rows have equal sort terms, Views now breaks ties by encoded row key in the same order as SQLite. This can change which rows `top` emits and how floating-point sums round when keys contain supplementary Unicode characters. SQLite uses the same stored bytes for keys and the same table layout.

If a view already wrote affected results, raise its circuit's `generation` to rebuild from retained input history. Point readers at the new generation once it catches up. [Rebuild a view](https://streamsy.dev/docs/views/lifecycle)
