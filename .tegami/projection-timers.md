---
packages:
  "@streamsy/projection": minor
---

## Request a process wake with its checkpoint

Return `Projection.wakeAt(time)` from a `Projection.process` handler to record its
next wake, using epoch milliseconds or an Effect `DateTime`. Returning nothing
clears it. Use `Commit.atLeastOnce` or `Commit.transaction`, including compensation
wrappers, so the request commits with the checkpoint.

A due unit carries the recorded time as `unit.due`, and its key includes that time
for idempotent retries. Without new input it has empty slices and ranges.
`Progress.wakeAt` reports the next recorded deadline. `run`, `pass` and `follow`
handle due timers when they run; each cycle makes at most one due unit without
input. Hosts waking at the deadline follow in the same release.

See [process timers](https://streamsy.dev/docs/projections/process#timers).
