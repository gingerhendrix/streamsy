---
packages:
  "@streamsy/storage": minor
---

## MySQL storage transactions default to READ COMMITTED

**Breaking change:** On MySQL, each plain read inside
`CommitBoundary.withTransaction` now takes a fresh snapshot. The boundary selects
`READ COMMITTED` for its transaction regardless of the session, pool or global
default, including `SERIALIZABLE`. Locking reads cannot lock absent rows or
guard a range against inserts at this level. Use a unique key or have writers
lock an existing parent row.

For Views on MySQL, this level lets a circuit in a host transaction read
operator state consistent with the checkpoint when another host advances the
binding. This covers transactions with or without an earlier application read
and transactions that run two circuits. It also lets `Projection.forget` see a
routed binding's first checkpoint when another host commits it between the
forgetting transaction's first read and its checkpoint load. Another host can
also commit a new binding of the same projection during a transaction that
calls `Projection.bindings`.

Set Effect's `SqlClient.BeginTransaction` to `"START TRANSACTION"` to use the
session default. Opting out to `REPEATABLE READ` brings back the stale Views and
`forget` results in those sequences.

Storage sends `SET TRANSACTION` and `START TRANSACTION` in one request. The
setting applies to that transaction only. Keep the MySQL session in autocommit.
Effect's mysql2 client enables multi-statement requests by default. A custom
client without multi-statement support fails at its first storage transaction.
The mysql2 `-MULTI_STATEMENTS` flag disables that capability.

With binary logging enabled, InnoDB writes in these transactions require
`binlog_format=ROW` or `MIXED`. In mixed mode MySQL logs those writes by row.
See the [MySQL isolation-level reference](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html)
and the [storage transaction guide](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).
