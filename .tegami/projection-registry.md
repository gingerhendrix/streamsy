---
packages:
  "@streamsy/projection": minor
---

## Observe attached projection runners

Provide the optional `Projection.Registry.layer` below `Projection.attach` layers to inspect live runners through `registry.status` and `registry.changes`. Status includes runner retry state, active and queued bindings, the last completed pass time, and disjoint durable mark counts. Changes emits an initial snapshot and full registry snapshots after passes and lifecycle changes. Runners unregister when their attach scope closes.
