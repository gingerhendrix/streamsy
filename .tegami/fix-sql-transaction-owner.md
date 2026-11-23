---
packages:
  "@streamsy/storage": patch
  "@streamsy/projection": patch
---

## Reject nested transactions across SQL owners

`CommitBoundary.withTransaction` and storage mutations now raise a defect if they enter a different SQL client’s active transaction. This also applies when a `Commit.transaction` handler runs or retires a binding on another SQL owner, or forgets it through that owner’s transaction. Previously, those calls could lose atomicity silently and leave writes behind after a rollback. Run work for the second owner after the first transaction finishes. Calls that share a `SqlClient` still join the outer transaction. See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).
