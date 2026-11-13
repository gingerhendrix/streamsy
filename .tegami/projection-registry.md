---
packages:
  "@streamsy/projection": minor
---

## Observe attached projection runners

Provide the optional `Projection.Registry.layer` below `Projection.attach` layers to inspect live runners through `registry.status` and `registry.changes`. Status includes runner retry state, active and queued bindings, the last completed pass time, and disjoint durable mark counts. Changes emits an initial snapshot and full registry snapshots after passes and lifecycle changes. Runners unregister when their attach scope closes.

The registry keys runners by projection id. Attaching the same id twice to one registry dies at layer build with an error naming the id.
