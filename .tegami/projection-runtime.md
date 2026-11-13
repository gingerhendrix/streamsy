---
packages:
  "group:streamsy": minor
---

## Observe, retry and trigger attached projections

`Projection.Registry` is a new in-process view of the runners that `Projection.attach` and `ProjectionWakeup.attach` start. Use it to check runner health, rerun failed bindings, and wait until a projection has read your own write. This release also adds trigger streams, which request projection runs from a wake stream, and accepted streams, which publish how far a projection has read each input. Every change is additive, so existing code needs no changes.

To use the registry, provide `Projection.Registry.layer` below your attach layers, so that every attachment registers with the same registry. On a Durable Object, put the layer in the object's `layer` next to `CheckpointStore.layer`. Each runner registers under its projection id and unregisters when its attach scope closes. Two attachments with the same projection id in one registry fail the layer build with an error that names the id. See [Observe attached runners](https://streamsy.dev/docs/projections/run#observe-attached-runners).

The registry has these members:

- `status` returns a `RunnerStatus` for each runner. It reports `state` (`running`, `retrying` or `failed`), the count of consecutive failed passes, `retryAt`, the `active` and `queued` bindings, and `lastPassAt`. A durable runner also reports `marks`, with separate `fresh`, `retryDue` and `backingOff` counts. `status` fails with `StorageFault` when it cannot read the mark counts.
- `changes` emits the full status list when you subscribe, and again after each pass, registration and retry change. A slow subscriber holds at most one pending snapshot. The stream logs and skips a failed read, so it never fails.
- `schedule(binding)` queues a binding and wakes its runner without another source write.
- `retry(target)` makes failed bindings due now and returns how many it selected. Pass a projection id or a declaration to select every failed binding, or a binding to select one.
- `events` emits a `RunnerEvent` for each binding run as it happens. `Ran` carries the run status, the unit count and the input offsets. `Failed` carries the attempt count and, for a durable binding, its retry deadline. `Dropped` marks a binding whose source was removed before it ran.
- `awaitOffset(binding, input, offset)` waits until the binding's checkpoint for `input` reaches `offset`. Use it to read your own writes in the same process.

`schedule` and `retry` end the runner's current wait and start a pass at once, even after a finite retry policy has run out. The runner keeps its retry schedule, so if the pass fails again, the runner waits for its next delay. Both methods fail with `NotAttached` when no runner has that projection id.

`events` has no replay. A subscriber sees only events published after it subscribes. The stream buffers without a limit, so close each subscription when you are done with it. `Projection.Registry.isFor(binding)` is a filter for the events of one binding. `RunnerEvent` exports its constructors, `$is` and `$match`. `awaitOffset` has no timeout, so wrap it in `Effect.timeout`.

To request runs from a wake stream, pass `triggers: [FolderWake]` to `Projection.attach` or `ProjectionWakeup.attach`. Each upsert in the trigger stream names a source stream, and the runner runs the binding that reads that source. The source can live on another storage owner. With `durable: true`, the runner commits its trigger cursor and its pending marks in a single transaction, so requested work survives a restart. With `durable: false`, a stop before the binding runs loses the request. A trigger is a wake class with `params: {}`, typed `WakeOutput<{}>`. `WakeOutput<P>` is now the exported type for wake classes. The folder-sync example indexes each folder as its own triggered binding, so a failing folder backs off alone. See [Trigger streams](https://streamsy.dev/docs/projections/run#trigger-streams).

To publish how far a projection has read, set `accepted` to a wake class on the declaration. Each unit's commit then writes a wake row for every input that moved, at the offset the unit accepted. For a State table, write `StateTable.define(State, { accepted: TotalsAccepted })`. `accepted` works with `Commit.transaction` and `Commit.producer` only. A fold with `accepted` must set one of them as its `commit`, and any other value is a type error.

In `@streamsy/core`, `Streamsy.awaitWake(Wake, params, { stream, offset })` waits until the wake row for `stream` reaches `offset`, over local or remote storage. With an accepted stream, a writer can wait until a projection on another owner has read its append. Each call reads the wake stream from the start. To read only later rows, pass `{ from: cursor }` with a saved wake cursor as the fourth argument. The same argument takes the `followWake` options. See [Accepted stream](https://streamsy.dev/docs/projections/accepted).

In `@streamsy/serve`, a `Wakeup` can implement `bind(host)`. The Durable Object host calls it once at acquire with a `WakeupHost`. `host.reconcile` recomputes the alarm, `host.retry` clears the wakeup's backoff floor, and `host.state` reads `failures` and `retryAt`. An optional `changed` effect runs after each attempted run. `ProjectionWakeup` uses these hooks to support the registry on a Durable Object. See [Projections on alarms](https://streamsy.dev/docs/servers/cloudflare/alarms#projections).

Memory and SQL storage implement two new operations on the internal `ProjectionMarksApi`: `summary`, which counts a projection's marks, and `retry`, which makes failed marks due now. The storage contract in `@streamsy/core/testing` checks both, so a custom storage adapter that supports durable marks must implement them. This release adds no storage migration.
