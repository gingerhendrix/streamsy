---
packages:
  "@streamsy/projection": minor
---

## Schedule a projection family and rescan its trigger history

`registry.schedule` now accepts an array of bindings of one projection, validates
them all before writing marks, coalesces duplicates and wakes the runner once.
Empty arrays do nothing; mixed projection ids fail with `RangeError`. A storage
failure in a later chunk can leave earlier marks written; repeating the request is safe.

After a fold fix, call `registry.rescan(projectionOrId, WakeClassOrStreamId)` to reset
one trigger cursor to the start, clear its stop and wake the runner. The reset is
stored in both durability modes. A first attach already reads from the start, so a
new family needs no rescan. Both requests work on the process and alarm hosts.
See [runner requests](https://streamsy.dev/docs/projections/run#observe-attached-runners).
