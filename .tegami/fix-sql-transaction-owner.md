---
packages:
  "@streamsy/storage": patch
  "@streamsy/projection": patch
---

## Give nested SQL clients their own transactions

In 0.13.0, a nested `CommitBoundary.withTransaction` on a second SQL client ran its body without a transaction. A failure could leave earlier writes behind. The inner boundary now opens its own transaction, which commits first and survives an outer rollback. Non-transactional projections on the second client now save their checkpoints in a transaction. Durable Object clients still share the driver's transaction.

Open the boundary first. A raw transaction on that client may be nested inside it. A mutation in a second client's raw `sql.withTransaction` used to run while only the first client's boundary was open. It now dies. A fiber or finalizer that outlives the body and uses its old transaction context now dies when it tries to enter the boundary or mutate storage. See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).
