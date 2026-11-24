---
packages:
  "@streamsy/projection": minor
---

## Commit projections on hosts without SQL

Use `Commit.ownerTransaction` with `CheckpointStore.layer` and `OwnerTransaction.layer` to commit projection output and its checkpoint together on a memory storage host. On a SQL storage host, either transaction layer supplies the service for `Commit.ownerTransaction`. `Transaction.layer` provides both services from the same storage owner. Use `Commit.ownerTransactionWithCleanup` to run binding cleanup in the same transaction as `Projection.forget` or `Projection.retire`. [Commit reference](https://streamsy.dev/docs/reference/projection/commit#commitownertransaction)
