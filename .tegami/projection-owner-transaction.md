---
packages:
  "@streamsy/projection": minor
---

## Commit projections on hosts without SQL

Use `Commit.ownerTransaction` with `CheckpointStore.layer` and `OwnerTransaction.layer` to commit projection output and its checkpoint together on a memory storage host. You can use the same commit on a SQL storage host. Use `Commit.ownerTransactionWithCleanup` to run binding cleanup in the same transaction as `Projection.forget` or `Projection.retire`. [Commit reference](https://streamsy.dev/docs/reference/projection/commit#commitownertransaction)
