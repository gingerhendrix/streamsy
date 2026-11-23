---
packages:
  "@streamsy/storage": minor
---

## MySQL storage transactions default to READ COMMITTED

**Breaking change:** A host that relied on repeatable reads inside
`CommitBoundary.withTransaction` on MySQL now sees a fresh snapshot on each
plain read. A begin statement supplied through Effect's
`SqlClient.BeginTransaction` still takes precedence.

When another host advances the same binding, a Views circuit inside a host
transaction now reads operator state consistent with the checkpoint. This
covers transactions with or without an earlier application read and
transactions that run two circuits. `Projection.forget` can now see and clear a
routed binding's first checkpoint and route records when another host commits
them after the forgetting transaction's first read.

Storage sends `SET TRANSACTION` and `START TRANSACTION` in one request. The
setting applies to that transaction only. Effect's MySQL client enables
multi-statement requests by default. Keep that capability enabled when
configuring a pool or supplying your own client.

With binary logging enabled, InnoDB writes in these transactions require
`binlog_format=ROW` or `MIXED`. In mixed mode MySQL logs those writes by row.
See the [MySQL isolation-level reference](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html)
and the [storage transaction guide](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).
