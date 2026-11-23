---
packages:
  "@streamsy/views": minor
---

## Views runs on Postgres

Set up a Postgres `SqlClient` with `SqlStorage`. Provide `CheckpointStore.layer`,
`OwnerTransaction.layer`, and `ViewBackend.layerSql` over that storage layer.
Run `View.migrate(sql)` at host
startup before running Views circuits. Concurrent hosts can run the migration
against the same database.

Views adds no length limit for row keys or sort keys on Postgres. A snapshot
arrangement identifier must fit in 255 UTF-8 bytes, including the source's State
key and collection name. An identifier that exceeds the bound or a row or owner
key with a lone surrogate fails the unit with a non-retryable `ViewDataFault`.

[Run on Postgres](https://streamsy.dev/docs/views/postgres).
