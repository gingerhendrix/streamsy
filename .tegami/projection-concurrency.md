---
packages:
  "@streamsy/projection": minor
---

## Run projection bindings in parallel

Pass `concurrency` to `Projection.attach` or `ProjectionWakeup.attach` to run several
bindings at once. The default is `1`; each binding still runs its units in order,
and a failed binding backs off alone. The option must be a positive safe integer.

SQLite serializes `Commit.transaction` handlers; Postgres and MySQL run them up to
the pool size. `Commit.atLeastOnce` handlers run in parallel on every host. Keep
concurrency below the pool size, use the provided `SqlClient` in the handler's fiber,
and append to shared outputs last. See [binding concurrency](https://streamsy.dev/docs/reference/projection/runners#binding-concurrency).
