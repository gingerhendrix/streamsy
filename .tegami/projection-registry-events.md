---
packages:
  "@streamsy/projection": minor
---

## Observe attached binding runs through registry events

`Projection.Registry.events` emits live `RunnerEvent` observations from Bun attachments and Durable Object projection wakeups: `Ran` includes status, unit count and input offsets; `Failed` includes binding attempts and an optional retry deadline; `Dropped` identifies a source removed before its first run. Events have no replay and use unbounded buffering so slow subscribers never block projection passes. Scope subscriptions to release buffered events when finished.

For in-process read-your-writes, `registry.awaitOffset(binding, input, offset)` subscribes before checking `Projection.read`, then waits until that input checkpoint reaches the target. It fails with `NotAttached` or the checkpoint read’s error and has no timeout; add `Effect.timeout`. Non-durable `Failed` events publish immediately with `dueAt: None`; those bindings retry with their pass, at `RunnerStatus.retryAt`. Durable events carry their mark’s retry deadline.

`RunnerEvent` exports tagged-enum constructors, `$is` and `$match`. Each event carries the binding’s encoded param record; `Projection.Registry.isFor(binding)` matches its projection id and params.
