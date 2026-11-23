---
packages:
  "@streamsy/storage": patch
  "@streamsy/projection": patch
---

## Reject nested transactions across SQL owners

`CommitBoundary.withTransaction` now raises a defect when nested across different SQL clients. This includes running or retiring a projection on another owner or forgetting it through that owner’s transaction. Previously, those calls could leave writes behind after a rollback. Run the second owner’s transaction after the first finishes.

Calls that share a `SqlClient` still join the outer transaction. A lone `Storage.mutate` on another client with no ambient transaction still opens and commits its own transaction. A raw ambient transaction needs a matching `CommitBoundary` on its own client before a storage mutation can write. See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).
