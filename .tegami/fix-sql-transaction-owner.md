---
packages:
  "@streamsy/storage": patch
  "@streamsy/projection": patch
---

## Give nested SQL clients their own transactions

In 0.13.0, nesting `CommitBoundary.withTransaction` on a second SQL client ran the inner body without a transaction, so a failure could leave partial writes behind.

- **Independent inner transactions:** A nested `CommitBoundary.withTransaction` on a second SQL client now opens its own transaction, which commits first and survives an outer rollback. Non-transactional projections running on the second client now save their checkpoints inside a transaction. Durable Object clients still share the driver's transaction.
- **Stricter boundary and lifetime checks:** Open `CommitBoundary.withTransaction` before nesting raw `sql.withTransaction` on that client. Mutating a second client inside its raw `sql.withTransaction` while only the first client's boundary is open now dies instead of running. Fibers or finalizers that outlive a transaction body and reuse its old context now die if they attempt to enter the boundary or mutate storage.

See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).
