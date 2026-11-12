---
packages:
  "@streamsy/projection": patch
---

## Retain storage-fault backoff for Bun registry requests

Schedule and retry now follow the same storage-fault rule on Bun and Durable Objects. They keep a storage-fault pass delay, while still interrupting binding-failure backoff. Blocked retry returns zero without changing marks or recording retry requests. Schedule records work, including its durable mark, for the pass after backoff. An exhausted storage-fault policy stays stopped.
