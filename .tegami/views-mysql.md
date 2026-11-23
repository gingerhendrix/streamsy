---
packages:
  "@streamsy/views": minor
---

## Views runs on MySQL

Use a MySQL `SqlClient` with `SqlStorage`, `CheckpointStore.layer` and
`Transaction.layer`. Install `@streamsy/storage` at the same version as
`@streamsy/views` for the MySQL transaction isolation used by Views.
Run `View.migrate(sql)` at host startup before running Views circuits.
Concurrent hosts can migrate the same database. Tested on MySQL 8.4.11.

A snapshot arrangement identifier must fit in 255 UTF-8 bytes, including its
prefix and the encoded State key and collection name. An overlong identifier
or a row or owner key with a lone surrogate fails the unit with a
non-retryable `ViewDataFault`. Views adds no length limit for row keys or sort
keys.

See [Declare and run a Views circuit](https://streamsy.dev/docs/views/declare-and-run)
for host setup.
