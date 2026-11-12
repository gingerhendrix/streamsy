---
packages:
  "@streamsy/projection": minor
---

## Observe attached binding runs through registry events

`Projection.Registry.events` emits live `RunnerEvent` observations from Bun attachments and Durable Object projection wakeups: `Ran` includes status, unit count and input offsets; `Failed` includes binding attempts and an optional retry deadline; `Dropped` identifies a source removed before its first run. Events have no replay and use unbounded buffering so slow subscribers never block projection passes. Scope subscriptions to release buffered events when finished.

For in-process read-your-writes, subscribe before checking `Projection.read`, then wait for the binding's input offset to reach the append receipt. `Failed.dueAt` is `None` when no retry is scheduled, including an exhausted finite retry policy.
