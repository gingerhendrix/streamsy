---
packages:
  "@streamsy/projection": minor
---

## Use one transaction commit on SQL and memory hosts

Compose `Transaction.layer` and `CheckpointStore.layer` over the same storage
owner. `Commit.transaction` commits the unit and its checkpoint together.
`Commit.transactionWithCleanup` also runs cleanup in the transaction used by
`Projection.forget` or `Projection.retire`.

**Breaking change:** `Transaction` loses its `sql` field. Read `SqlClient` from
context where you need SQL. The cleanup callback now takes just the checkpoint
key. Running a projection with a SQL handler requires the storage owner’s
`SqlClient` in the runner’s context. Use `Layer.provideMerge` to retain it when
composing the host. This also applies to `StateTable` and `FoldByKey` runners.

The transaction checks the checkpoint owner on every unit, including units
without routed outputs. Use the `load` function supplied by `CheckpointStore.layer`.
Wrapping `load` drops the owner metadata and fails with `unsupported-composition`.

[Commit reference](https://streamsy.dev/docs/reference/projection/commit#committransaction)
