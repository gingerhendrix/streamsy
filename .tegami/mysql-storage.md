---
packages:
  "@streamsy/storage": minor
  "@streamsy/projection": minor
---

## List projection bindings and routed outputs on MySQL (schema v4)

`Projection.bindings`, startup binding discovery for attached projections, and `Projection.retire` / `Projection.forget` for bindings with routed outputs now work on MySQL. Durable pending marks on MySQL also now support encoded params longer than 512 code points, and both attach hosts reject MySQL projection IDs longer than 255 code points when durability is enabled.

**Breaking change (MySQL schema v4):** Opening a MySQL database adds `full_key` to `streamsy_owner_records` and `full_params` to `streamsy_projection_pending`, upgrading MySQL storage to schema version 4. Streamsy 0.13.0 cannot open a MySQL database at version 4. SQLite and Postgres remain at schema version 3.

**Upgrade steps for MySQL:**

- Stop every 0.13.0 process before opening the database with this release. A running 0.13.0 process can read the fitted form of a long pending mark, fail to decode it, and delete the mark without running the binding (leaving it idle until its next commit).
- Any MySQL owner key longer than 512 code points written by 0.13.0 becomes listable again after its next write; until then, listing a range containing that key fails.

See [MySQL owner record listing](https://streamsy.dev/docs/reference/storage/sql-storage#mysql-owner-record-listing).

## MySQL storage transactions default to `READ COMMITTED`

**Breaking change:** On MySQL, `CommitBoundary.withTransaction` now sets `READ COMMITTED` for each transaction regardless of the session, pool, or global default (including `SERIALIZABLE`), so every plain read takes a fresh snapshot. At this isolation level, locking reads cannot lock absent rows or guard ranges against inserts—use a unique key or lock an existing parent row instead.

This isolation level ensures consistent reads across concurrent hosts:

- Views circuits in a host transaction read operator state consistent with checkpoints when another host advances the binding (with or without earlier application reads, and when running two circuits in one transaction).
- `Projection.forget` sees a routed binding's first checkpoint committed concurrently by another host.
- `Projection.bindings` sees newly committed bindings from other hosts during the same transaction.

**Configuration and requirements:**

- **Opting out:** Set Effect's `SqlClient.BeginTransaction` to `"START TRANSACTION"` to use the session default. Running under `REPEATABLE READ` reintroduces stale Views results in the sequences above and causes `Projection.forget` to fail with a retryable `token-conflict` if its snapshot predates a new checkpoint.
- **Multi-statement requests:** Storage sends `SET TRANSACTION` and `START TRANSACTION` in one request (per-transaction only; keep the session in autocommit). Effect's `mysql2` client enables multi-statement support by default, whereas passing `-MULTI_STATEMENTS` or using a custom client without it fails at the first transaction.
- **Binary logging:** With binary logging enabled, InnoDB writes in `READ COMMITTED` transactions require `binlog_format=ROW` or `MIXED` (logged by row in mixed mode).

See the [MySQL isolation-level reference](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html) and [MySQL transactions](https://streamsy.dev/docs/reference/storage/sql-storage#mysql-transactions).
