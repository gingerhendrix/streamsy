---
packages:
  "@streamsy/storage": patch
  "@streamsy/projection": patch
---

## Give nested SQL clients their own transactions

`CommitBoundary.withTransaction` now opens a transaction when called inside a boundary on another SQL client. Running, retiring, or forgetting a projection through that client's transaction uses the same rule. An inner failure rolls back the inner transaction. It also rolls back the outer one if the failure escapes the outer body. The inner client commits first, and that commit survives an outer rollback. There is no atomic commit across the two clients. Non-transactional projections can also save their checkpoints inside an outer boundary on another client.

Calls on the same client join without a savepoint. With two boundaries on one client, only the outer boundary's hooks and `committed` subscribers see the commit. Use one client per database for nested work. With two clients on one database, SQLite can wait until it reports a busy lock. Postgres or MySQL can wait for a row lock held by the outer transaction.

Durable Object clients share the driver's transaction, so a second client joins the first. Using a second object's storage on one fiber is unsupported. `MemoryStorage` dies when work enters another owner. Keep the transaction body on its calling fiber. A boundary call or mutation inside raw `sql.withTransaction` needs a matching boundary on that client. See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).
