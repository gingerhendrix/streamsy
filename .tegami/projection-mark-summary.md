---
packages:
  "@streamsy/core": minor
  "@streamsy/storage": patch
---

## Summarize durable projection marks

The internal `ProjectionMarksApi.summary(projection, now)` read returns disjoint fresh, due retry and backing-off counts, plus the earliest due time. Memory, SQLite, Durable Objects, Postgres and MySQL implement the same summary without changing mark or retry semantics.
