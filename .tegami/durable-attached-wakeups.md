---
packages:
  "group:streamsy": minor
---

## Durable pending marks for attached projections

`Projection.attach` and `ProjectionWakeup.attach` take `durable` and `batch` options. In durable mode, the commit that writes an input also writes a pending mark for each binding it touches, in the same transaction. A binding that catches up deletes its mark by a `seq` compare, so a write during the run keeps it. After a restart, an eviction or a deploy, the runner finds the marks and runs those bindings. A failed binding defers its own mark (1 s doubling to 1 hour) and no longer puts its neighbours on backoff.

`ProjectionWakeup.attach` is durable by default. `Projection.attach` stays in memory unless you pass `durable: true`. Durable mode needs a local memory or SQL storage layer and fails at acquisition over remote storage.

Storage migration 2 adds the `streamsy_projection_pending` table. A database opened by this release cannot be opened by an older one. `Wakeups.status` reports a `deferred` count for each wakeup.
