# @streamsy/projection

Checkpointed projections over typed Streamsy routes. A constructor defines the
step. Its `commit:` value defines how results and offsets become durable.

## Constructors

| Constructor                | Handler                  | Result                     | Commit                          |
| -------------------------- | ------------------------ | -------------------------- | ------------------------------- |
| `Projection.fold`          | `(state, entry, unit)`   | Next value                 | Defaults to `Commit.checkpoint` |
| `Projection.process`       | `(batch, unit, context)` | Effect of void or `WakeAt` | Required                        |
| `Projection.emit`          | `(entry, unit)`          | Output items               | Required                        |
| `Projection.mapAccum`      | `(state, entry, unit)`   | `[state, emits]`           | Required                        |
| `Projection.mapAccumBatch` | `(state, batch, unit)`   | `[state, emits]`           | Required                        |

Except for `process`, steps return a value or an Effect. A missing output key
emits nothing. `Projection.each` adapts an entry handler for `process`.
Batch handlers must give the same final rows for any unit size. In per-entry
steps, use `unit` for parameters and unit ranges.

A local input's `Slice.offsets` holds one storage offset per item, in item order;
its last offset equals `nextOffset` when non-empty. `Entry.offset` carries the
same offset, and a read from `entry.offset` resumes after that entry. Non-empty
HTTP inputs have `offsets` undefined and entries omit `offset`, because HTTP
supplies page boundaries only. Every empty slice has `offsets: []`, including
skipped inputs and timer-only units. Pinned retries retain the entry offsets.

```ts
import { Effect, Layer, Schema } from "effect";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";

class Numbers extends Streamsy.json<Numbers>()("Example/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}
class Doubled extends Streamsy.json<Doubled>()("Example/Doubled", "doubled", {
  params: {},
  schema: Schema.Finite,
}) {}
const doubled = Projection.emit({
  id: "doubled",
  params: {},
  inputs: { input: Numbers },
  outputs: { output: Doubled },
  commit: Commit.producer,
  emit: ({ item }) => ({ output: [item * 2] }),
});
const host = CheckpointStore.layer.pipe(
  Layer.provideMerge(MemoryStorage.layer(Streamsy.group(Numbers, Doubled))),
);
const program = Effect.gen(function* () {
  const numbers = yield* Numbers;
  yield* numbers.create({});
  yield* numbers.append({}, [1, 2, 3]);
  return yield* Projection.run(doubled.at({}));
});
await Effect.runPromise(program.pipe(Effect.provide(host)));
```

## Commits

| Commit        | Fits                                | Guarantee                        | Services                         |
| ------------- | ----------------------------------- | -------------------------------- | -------------------------------- |
| `checkpoint`  | `fold`                              | Exactly once for the saved value | `CheckpointStore`                |
| `transaction` | All constructors                    | Exactly once within SQL          | `CheckpointStore`, `Transaction` |
| `producer`    | `emit`, `mapAccum`, `mapAccumBatch` | Effectively once for outputs     | `CheckpointStore`, output routes |
| `atLeastOnce` | `process`                           | At least once                    | `CheckpointStore`                |

`Commit.transaction` provides `SqlClient`. Its SQL, same-owner stream appends,
and checkpoint commit together. External calls are at least once. Memory hosts
provide no `Transaction`.

`Commit.producer` pins ranges and producer sequences. Retries append with the
same positions; handlers must be deterministic. A missing output on the
checkpoint's owner is created on the first append. An output on another owner
must already exist: its owner creates it at start. There is no atomicity
across producer outputs. Chain projections for sinks with different guarantees.

`Commit.atLeastOnce` pins before processing, so retries keep `unit.key`.
`Projection.process` with `Commit.transaction` can use `prepare(batch, unit)`
outside the transaction. It runs at least once and passes its result to
`process`. A retry can read new input and change `unit.key`.

`Commit.transactionWithCleanup((key, sql) => cleanup)` passes the binding's
checkpoint key and transaction SQL client to the cleanup Effect.
`Projection.forget` runs cleanup after deleting any live checkpoint.
`Projection.retire` runs it after the tombstone write and owned output cleanup.
The callback also runs on a repeated retirement. A cleanup failure rolls back
changes in the same transaction, including SQL run with the supplied client.

`Commit.withCompensation(f)` calls `f(result, cause, unit)` after successful
processing if the commit fails. It is also available as a `.pipe` combinator.
For external delivery, use `Commit.atLeastOnce` with `unit.key` as the idempotency key,
the declaration's [binding retry policy](https://streamsy.dev/docs/reference/projection/runners#binding-retry)
for a retry budget and `onExhausted`, and [`Projection.wakeAt`](https://streamsy.dev/docs/projections/process#timers)
for a deferred retry or a snooze. See the [effects guide](https://streamsy.dev/docs/projections/effects) for the complete pattern.

To stop a binding on a permanent typed handler error, add
`readonly [NonRetryable] = true` using `NonRetryable` from `@streamsy/projection`.
Attached runners report the failure without stepping the retry schedule or
calling `onExhausted`.

`Commit.make` defines `load`, `apply`, and optional `remove`. Custom Commits
extend ranges on retry. Never save after a failed handler; compare-and-set on
`loaded.token`; report `checkpoint / token-conflict` for a moved token; return
`"0"` for an absent record; and load stored state. The guarantee must describe
the actual writes. See the [bucket example](https://streamsy.dev/docs/projections/state#custom-commits).

## State and output routes

`fold`, `mapAccum`, and `mapAccumBatch` require `state: { schema, initial }`.
`initial` can depend on decoded parameters. State and offsets share one record.
`Projection.read(bound)` reads through the Commit's `load` and returns
`{ state, offsets }`, using `initial` before the first unit.
`Projection.current(bound)` folds the checkpoint and input tail in memory. It
takes no lock and writes no checkpoint or outputs. It requires `state:` and
returns matching state and offsets, including entries appended before the call.
`mapAccum` and `mapAccumBatch` compute state and discard emitted items.
`Projection.decide(bound, { input, decide })` requires `state:`, calls `current`,
decides, and appends to one input with that input's offset as `expectedOffset`. It
retries on `OffsetMismatch`. See [Read, decide, append](https://streamsy.dev/docs/projections/state#read-decide-append).

`Serve.value(schema, effect)` serves values computed with `Projection.read`.
Before the first saved unit, reads return the declared initial value.
`Serve.mount(Board, path)` and `Serve.mount(Transitions, path)` mount output routes.

Outputs accept routes or fixed addresses, like inputs. `Streamsy.json` accepts
items. `Streamsy.state` accepts `DurableState.upsert("card", row)` and
`DurableState.remove("card", key)` from `@streamsy/core`. Each change names its
collection. Two outputs cannot name the same stream or an input.

## State tables

`StateTable.define(State)` declares a projection that keeps the rows of a
`Streamsy.state` stream in SQLite tables beside the stream. Run
`StateTable.migrate(sql)` at host start and attach the table with
`Projection.attach(table, { durable: true })`. A bound table reads rows with
`get`, `getMany` and `list`, each with the offset that the rows reflect.
`StateTable.rebuild` makes the rows again from the stream alone. The types of
a declaration and a bound table are `StateTable.Declaration` and
`StateTable.Bound`.

`FoldByKey.process(table, spec)` builds a `Projection.process` handler, under
`Commit.transaction` with the State stream in `outputs`, that folds records into
one collection. It reads each key's previous row from the table. `sync` and the
fold handler fail with `ProjectionFault` for every wire, SQL and schema failure,
and the handler adds `FoldByKeyError` for a row with the wrong key. A fold's
identity includes its `id`, `generation` and binding params. See
[State tables](https://streamsy.dev/docs/projections/state-table).

## Wake streams

`Projection.wake` writes a `Streamsy.wake` stream from one source stream. It is
`mapAccumBatch` with `Commit.producer`. Each unit appends one upsert
`{ stream, offset, closed? }`, so several source writes in one unit fold into
one row. It reads committed storage, so every write path counts: toolkit calls,
HTTP, other projections and raw `StreamsWriter` writes.

```ts
const filesWake = Projection.wake({
  id: "files-wake",
  params: { user: UserKey, folderId: FolderKey },
  input: Files,
  output: UserWake,
});
```

Here `Files.at` takes `user` and `folderId`, and `UserWake.at` takes
`user`. Each Files id supplies both params for attachment. The wake stream can live
on another owner, where its owner must create it before the first append. The producer
creates a missing output on its checkpoint owner only. A retry answers `Duplicate`, so delivery is effectively
once for each binding. Rows of one source arrive in source order. A reader that
comes before the first append gets 404 unless the owner calls the normal State
service's `create` to make an empty stream available. `Streamsy.wake` is a State
preset with `wakeCollections`; a `Streamsy.state` declaration with those
collections also works as the output.

| Source event       | Wake change                                   |
| ------------------ | --------------------------------------------- |
| Append             | Upsert with the new tail, one for each unit   |
| Close with a body  | Upsert with `closed: true` in the same unit   |
| Close, no body     | Upsert with `closed: true` at the same offset |
| Remove, TTL expiry | State delete of the source id, then `forget`  |
| Create, empty      | None until the first append                   |

A removed or expired source answers `StreamGone` or `StreamNotFound`. A
binding with a checkpoint then appends the delete with its next producer
sequence and forgets itself. A stream id never returns, so the delete is final.
A binding with no checkpoint keeps the `history-unavailable` fault, because its
source never existed for it. A drained closed source reports `source-closed`.
`Projection.follow` ends when its input closes. Observe a later removal with
`Projection.attach` or another explicit run.

All projections receive terminal outcomes through their input slices. A close
with no new items still makes one unit with `slice.closed`; the checkpoint
records the close so subsequent runs do not repeat that empty unit. Batch
handlers (`process` and `mapAccumBatch`) can react even when `slice.items` is
empty; per-item handlers have no entries to process.

When an input with a checkpoint is confirmed gone, the kernel runs a final
unit with `slice.gone: true`, then forgets the binding by default. Lost history
on a live input still fails. Other storage faults fail the pass and remain
available to the normal retry policy.

A pending unit replays its pinned input ranges. If the input is removed
between the pin and the settle, the replay fails with `range-unreproducible`
and the pin stays. The final gone unit does not run for that binding. Keep
source history until pending units settle, or call `Projection.forget` on the
binding after you clean up its outputs.

## Hosts

| Layer                         | Provides                     | Requires                        |
| ----------------------------- | ---------------------------- | ------------------------------- |
| `CheckpointStore.layer`       | `CheckpointStore`            | `CommitBoundary`, owner records |
| `Transaction.layer`           | `Transaction`                | `CommitBoundary`, `SqlClient`   |
| `CheckpointStore.layerMemory` | Standalone `CheckpointStore` | Nothing                         |

`CheckpointStore.layer` keeps checkpoints on the owner of the streams. It works
over every Streamsy storage host: `MemoryStorage.layer`, `BunStorage.layer`, and
`DurableObjectStorage.layer`. Add `Transaction.layer` on a SQL host for
`Commit.transaction`. Compose the projection Layers over storage with
`Layer.provideMerge` and retain one Layer graph for the host lifetime:

```ts
const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layer(streams, { client: { filename } })),
);
```

`CheckpointStore.layerMemory` is standalone. It keeps checkpoints in the
process and does not join the stream owner, so processing is at least once. Use
it for rebuilt values, tests, and a remote host (`RemoteStreams.layer`).

### Attached projections

`Projection.attach(projection)` runs a projection whenever a local commit
writes one of its inputs. The storage reports the stream ids of each commit
through `Storage.committed`. For each id, the input's `parse` gives the params,
and the runner runs `projection.at(params)`. So a new member of a family gets
its binding on its first commit, and the app names no bindings.

```ts
const host = Layer.mergeAll(Api, Projection.attach(filesWake)).pipe(
  Layer.provideMerge(CheckpointStore.layer),
  Layer.provideMerge(BunStorage.layer(streams, { client: { filename: path } })),
);
```

Each input must give every projection param, so that one id picks one binding.
`attach` throws a `RangeError` when an input does not. Inputs on another owner
never commit here; use a wake stream in `triggers` to request their bindings. Do not also run an attached binding
under `Projection.follow`.

Pass fixed-address wake classes with `triggers: [Wake]`. A provided stream service
on another owner makes a trigger remote; a matching owner or absent service
uses local storage. Provide the remote class
at runtime with `Protocol.bind(RemoteStreams.layerProtocol(...), Streamsy.group(Wake))`
or another owner's protocol layer. `Projection.attach` starts a scoped live-read
follower for each remote trigger, so a remote upsert wakes it without a local
commit. `ProjectionWakeup.attach` rejects remote triggers at attach with a
`RangeError` naming the stream; it accepts only its own storage owner's triggers.

A pass reads local triggers, drains bindings, then reads one page per remote
trigger outside the local transaction. Durable marks and the page's `nextOffset`
commit together locally. Cursors persist in both durability modes; private marks
can be lost after ingestion on restart. Missing or deleted remote streams leave
the cursor unchanged without a fault. Invalid requests, data or page cursors stop
only that trigger with `invalid-source`; `registry.rescan` and projection-level
`registry.retry` clear the stop. Remote transport or storage read failures fail
the pass retryably after all remote triggers are attempted. Local commits during
pass backoff wait until it ends, up to the default 30 s cap. The follower uses its
own `passRetry` steps and never signals a failure. `next` makes no network request.
See [Trigger streams](https://streamsy.dev/docs/projections/run#trigger-streams).

By default `Projection.attach` keeps pending binding marks, timers and backoff in private memory.
In both modes a retryable failed binding waits alone, by default from 1 s doubling to 1 hour, while
its neighbours run. A non-retryable projection or storage fault stops only the binding that failed.
Handler errors and defects retry. A local commit or trigger row does not shorten backoff or end a stop;
`registry.retry` or `registry.schedule` makes it due now. `registry.failed` lists
failed bindings with their stored cause. A runner start makes stopped marks due once.
If the process stops after a commit and before the run, the binding is late
until its source gets its next write. The next run catches up from the
checkpoint while the source history remains available. Removal before that run
uses the final gone unit instead. A commit by another process on the same SQL
file does not show.

Pass `durable: true` to close that gap. Each commit then also writes a pending
mark for each binding it touches, in the same transaction, to
`streamsy_projection_pending`. A binding that catches up keeps a sleeping mark
when its checkpoint records a wake; otherwise it deletes its mark, unless a write
during the run marked it again. When the layer starts, it runs
the bindings with due marks, up to `batch` (default 25) per pass. Durable mode needs a local memory or SQL storage layer.

On Cloudflare, acquire a wakeup in the object's layer scope. Local commits
fill the same runner's pending set; `next` answers "now" while it has work,
and each alarm drains one pass (100 units per binding by default).
`ProjectionWakeup.attach` is durable by default, because Cloudflare evicts idle
objects. After an eviction or a deploy, the object's acquire reads the marks
and arms the alarm, including sleeping timer deadlines. Pass `durable: false` for private memory marks and timers that reset on eviction until the binding's next input.

```ts
StreamsyObject.make({
  app,
  layer,
  wakeups: [ProjectionWakeup.attach(filesWake)],
});
```

Both attach forms accept `concurrency` (default `1`), the most bindings run at once.
It must be a positive safe integer. Each binding still runs in at most one fiber;
units within that binding run in order.

| Host                        | `Commit.transaction`                                                                     | `Commit.atLeastOnce`                 |
| --------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------ |
| SQLite (Bun, Node and Deno) | Transaction bodies serialize; reads and `prepare` can overlap another unit's transaction | Handler bodies run in parallel       |
| Durable Objects             | As SQLite                                                                                | Handler bodies overlap across awaits |
| Postgres and MySQL          | Handler bodies run in parallel up to the pool size, subject to shared output row locks   | Handler bodies run in parallel       |

Keep `concurrency` below the pool size: each `Commit.transaction` unit holds one
pooled connection for its whole handler. Both server drivers default to 10 connections;
leave a connection for reads, appends and runner storage operations. Inside the handler, run all SQL on the
provided `SqlClient` in the handler's fiber. `sql.reserve` or a fiber without the
handler's transaction context waits forever on SQLite, and on a pool once every
connection holds a transaction. Appending to a shared output last keeps its row
lock short: units serialize from that append until commit. Units that lock streams
in opposite orders across separate appends can deadlock on Postgres or MySQL; the
database aborts one transaction and its binding backs off before retrying.

All five constructors accept `retry: { schedule?, maxAttempts?, onExhausted? }`.
The default binding retry doubles from 1 s to 1 hour without a limit. A finite schedule
or `maxAttempts` stops the binding when exhausted; `registry.failed` includes
`exhausted: true`. `registry.retry` permits one more run. New input during backoff
keeps the attempt count and deadline; success clears them. The runner replays a fresh
schedule for the failure count, so use step-count schedules, without input or elapsed-time
dependence. Stored deadlines survive durable restarts without recomputation.
`onExhausted(unit, failure)` runs at least once before stopping; dedupe with `unit.key`.
It receives decoded params. If no unit formed before the exhausting failure, the hook
is skipped and logged. A failing hook is logged and the binding stops anyway.
Non-retryable faults stop without the hook. Hooks run outside the handler transaction.
A `process` context includes `attempt`, starting at 1 and increasing on failed attached
runs. Direct `run` and `follow` always use 1. See [binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry).

Both forms accept `passRetry: Schedule.Schedule<unknown>`. Timing only: the runner calls
the schedule once per retryable failed pass with no input. Bun waits for its delays; the
alarm host uses them to re-arm. A dying schedule is logged and treated as exhausted. The default is capped, jittered exponential backoff.
An attach call that still passes the removed `retry` option throws a `RangeError` naming `passRetry`.
A finite schedule stops pass retries when exhausted. Binding failures use the declaration's `retry` policy; the pass policy paces failures in marks, cursor or trigger operations.

SQL hosts keep checkpoints in `streamsy_owner_records`. Older projection tables
are not migrated or read; projections rebuild from zero.
Layer authors use `EncodedStore` and `fromStore` from `@streamsy/projection/checkpoint`.

`Projection.Registry.layer` exposes attached runners through `status`, `changes`,
`events`, `failed` and `awaitOffset`. `registry.schedule(binding)` also accepts an
array of bindings of one projection and wakes the runner once. Mixed projection
ids die with `RangeError`; empty arrays do nothing and duplicates coalesce. Every
binding is validated before any marks are written. Invalid params fail with
`InvalidBinding`. Marks are written in chunks of 100; a later storage failure can
leave earlier chunks marked, so repeat the request safely. The call returns before
processing finishes. `registry.retry(target)` makes failed work due now.

After a fold fix, `registry.rescan(projectionOrId, WakeClassOrStreamId)` resets one
trigger cursor to `ZERO_OFFSET`, clears its stop and wakes the runner. It does not
reset binding checkpoints. Raise `generation` to replay those bindings.
A row that still fails to decode stops that stream again. The cursor is stored in
both durability modes and survives restart; ingested marks survive only in durable
mode. An unknown projection fails with `NotAttached`; an unknown trigger dies with
`RangeError`. A first attach already reads a trigger stream from its start, so a new
family needs no rescan. See [runner requests](https://streamsy.dev/docs/projections/run#observe-attached-runners).

## Timers

A `Projection.process` handler can return `Projection.wakeAt(time)`, with epoch
milliseconds or an Effect `DateTime`, under `Commit.atLeastOnce` or
`Commit.transaction` (including compensation wrappers). Returning void clears the
previous timer; new input before the deadline can replace or cancel it. Other
return values are discarded. A custom `Commit<void>` does not fit a timer handler.

When the recorded time is due, the handler receives `unit.due: Some(time)`. Without
new input, the slices and `unit.ranges` are empty. With new input, one unit carries
both the items and the due time. The key includes the recorded due time, so pinned
retries keep it and distinct deadlines have different keys. Decide what is due
from `unit.due`; read `Clock` only to choose a late policy.

`run`, `pass` and `follow` handle due timers when they next run. They do not wait
for the deadline; `follow` can discover it on an input change or repair cycle.
A `run` cycle makes at most one due unit without input, even if its handler returns
a time already passed. Return a time after `unit.due` for the next wake.

`Projection.attach` wakes a sleeping binding at its recorded deadline. Its process
wait re-reads the wall clock at least every 30 seconds while a deadline exists,
so after host suspend a passed wake is discovered within 30 seconds of resume.
`ProjectionWakeup.attach` exposes that deadline to the Durable Object alarm host,
which arms its alarm for the wake time. Wakes can be late; handlers use `unit.due`
to apply their late policy. A wake during backoff waits for backoff to end. A
stopped binding keeps its checkpoint timer and fires late after retry or restart.
With `durable: false`, pending work and timers live in private memory: restart or
eviction loses a timer's wake until the binding's next input. Use durable marks
on persistent storage for restart recovery.

Return a time after `unit.due` for the next wake. Repeatedly returning a passed
time leaves the mark due immediately and can run the handler at pass speed.

## Run and recover

`declaration.at(params)` binds a projection. `pass` runs one unit; `run` continues
until caught up, closed, or its optional unit limit. `follow` returns a `Stream`
that runs to caught up, then waits for changes, with a default repair interval of
30 seconds. Remote inputs work with `follow` and `run`.

`run` and each `follow` element give a `Progress`: the `state` and `offsets` that
the last pass left, plus `status`, `units`, `items`, and `wakeAt: Option<number>`. The stream
emits after the first cycle, after each cycle that commits a unit or reaches
`limit`, and after the last cycle when every input is closed; then it ends. A cycle
that finds no input or due timer emits nothing. Invalid options fail the stream at its first pull.

```ts
// Forks the follower into the caller's scope; closing the scope stops it.
const follower = Projection.follow(bound).pipe(Stream.runDrain, Effect.forkScoped);
```

The stream is pull based: the next cycle runs only when the consumer pulls, so a
slow consumer gets one element for all the input that arrived meanwhile. The
consumer's pace sets the follower's pace; add
`Stream.buffer({ capacity: 1, strategy: "sliding" })` to decouple them. A consumer
failure or interruption stops the follower. `Stream.share` fans one follower out to
several consumers and stops it after the last one exits; to keep a projection up
with no consumer, drain it and publish each element to a `SubscriptionRef`.

Use one writer per projection key: one running `follow` stream, or one `run` at a
time. Each run of a `follow` stream value is a new writer. Schedules must not overlap. A second writer is a deployment fault; a
moved checkpoint token fails with `token-conflict`. External handlers may have
already run. Any number of `current` and `decide` calls can run beside the
follower, including inside a caller transaction. Table and stateless output
projections are follow-only; wait on their output to see a command.

`follow` retries `token-conflict`, `output-busy`, and retryable `storage-failure`: `options.retry` overrides the
`ProjectionRetry` context reference, whose default is jittered exponential
backoff from 200 ms, capped at 30 s. `run` does not retry. Handler errors retain
their types; `ProjectionFault` carries `phase`, `reason`, and `cause`.

Records are keyed by `id`, `generation` and canonical parameters.
Raise `generation` for changed input identity or state schema. When migrating a
declaration that set `version`, remove that option and set `generation` above
every generation the projection id has used, including under older versions.
Deleting `version` alone can resume a previous version 1 checkpoint.

Retain input history after the checkpoint, including pinned ranges. Stop the
follower before calling `Projection.forget(bound)`, which calls the Commit's
`remove` and leaves output streams in place. A raised generation replays into
fresh `perGeneration` streams. An unwrapped `Commit.producer` output receives the
replay after its existing items. Move its address or remove that output stream
before replay if it should contain only the rebuilt items.

Without a follower, each `current` replays from the last checkpoint or from
the start. A continuously growing input can delay it. Fold handlers must be
replayable and give the same result for any page split.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## License

MIT
