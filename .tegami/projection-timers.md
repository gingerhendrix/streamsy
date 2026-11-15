---
packages:
  "@streamsy/projection": minor
  "@streamsy/core": minor
  "@streamsy/storage": minor
  "@streamsy/serve": minor
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
input. Attached process and Durable Object hosts wake at the deadline. A process
wait rechecks wall time every 30 seconds while a deadline exists, including after
host suspend. Backoff delays a wake; stops keep it until retry or restart. With
`durable: false`, restart or eviction loses the wake until the next input. Return
a time after `unit.due` to avoid repeatedly running an immediately due timer.

`RunnerStatus.marks.sleeping` counts sleeping bindings separately from pending and
failed work. `RunnerEvent.Ran.wakeAt` reports the next recorded wake. Schema
version 3 adds nullable `wake_at` to projection marks on every SQL host.

See [process timers](https://streamsy.dev/docs/projections/process#timers).
