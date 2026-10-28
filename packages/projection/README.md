# @streamsy/projection

Checkpointed projections over typed Streamsy routes. A constructor defines the
step. Its `commit:` value defines how results and offsets become durable.

## Constructors

| Constructor                | Handler                   | Result           | Commit                          |
| -------------------------- | ------------------------- | ---------------- | ------------------------------- |
| `Projection.fold`          | `(state, entry, unit)`    | Next value       | Defaults to `Commit.checkpoint` |
| `Projection.process`       | `(batch, unit, prepared)` | Effect of void   | Required                        |
| `Projection.emit`          | `(entry, unit)`           | Output items     | Required                        |
| `Projection.mapAccum`      | `(state, entry, unit)`    | `[state, emits]` | Required                        |
| `Projection.mapAccumBatch` | `(state, batch, unit)`    | `[state, emits]` | Required                        |

Except for `process`, steps return a value or an Effect. A missing output key
emits nothing. `Projection.each` adapts an entry handler for `process`.
Batch handlers must give the same final rows for any unit size. In per-entry
steps, use `unit` for parameters and offsets only.

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

`Commit.withCompensation(f)` calls `f(result, cause, unit)` after successful
processing if the commit fails. It is also available as a `.pipe` combinator.
For durable external delivery, use the [action outbox](https://streamsy.dev/docs/projections/actions).

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
`user`. Each Files id supplies both params for attachment. The wake stream can live on another owner. The producer creates it on
the first append, and a retry answers `Duplicate`, so delivery is effectively
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
  Layer.provideMerge(BunStorage.layer(streams, { path })),
);
```

Each input must give every projection param, so that one id picks one binding.
`attach` throws a `RangeError` when an input does not. Inputs on another owner
never commit here, so they never trigger. Do not also run an attached binding
under `Projection.follow`.

By default `Projection.attach` keeps the set of bindings with work in memory.
If the process stops after a commit and before the run, the binding is late
until its source gets its next write. The next run catches up from the
checkpoint while the source history remains available. Removal before that run
uses the final gone unit instead. A commit by another process on the same SQL
file does not show.

Pass `durable: true` to close that gap. Each commit then also writes a pending
mark for each binding it touches, in the same transaction, to
`streamsy_projection_pending`. A binding that catches up deletes its mark,
unless a write during the run marked it again. When the layer starts, it runs
the bindings that still have marks, up to `batch` (default 25) per pass. A
binding that fails waits alone, from 1 s doubling to 1 hour, and its
neighbours keep running. Durable mode needs a local memory or SQL storage
layer.

On Cloudflare, acquire a wakeup in the object's layer scope. Local commits
fill the same runner's pending set; `next` answers "now" while it has work,
and each alarm drains one pass (100 units per binding by default).
`ProjectionWakeup.attach` is durable by default, because Cloudflare evicts idle
objects. After an eviction or a deploy, the object's acquire reads the marks
and arms the alarm. Pass `durable: false` for the in-memory set only.

```ts
StreamsyObject.make({
  app,
  layer,
  wakeups: [ProjectionWakeup.attach(filesWake)],
});
```

Both forms accept `retry: Schedule.Schedule<unknown>`. Bun applies it with
`Effect.retry` to a drain pass; the alarm host uses its delays to re-arm instead
of sleeping inside an alarm. The default is capped, jittered exponential backoff.
A finite schedule stops retries when exhausted.

SQL hosts keep checkpoints in `streamsy_owner_records`. Older projection tables
are not migrated or read; projections rebuild from zero.
Layer authors use `EncodedStore` and `fromStore` from `@streamsy/projection/checkpoint`.

## Run and recover

`declaration.at(params)` binds a projection. `pass` runs one unit; `run` continues
until caught up, closed, or its optional unit limit. `follow` returns a `Stream`
that runs to caught up, then waits for changes, with a default repair interval of
30 seconds. Remote inputs work with `follow` and `run`.

`run` and each `follow` element give a `Progress`: the `state` and `offsets` that
the last pass left, plus `status`, `units`, and `items`. The stream
emits after the first cycle, after each cycle that commits a unit or reaches
`limit`, and after the last cycle when every input is closed; then it ends. A wake
that finds no input emits nothing. Invalid options fail the stream at its first pull.

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

`follow` retries only `storage-failure`: `options.retry` overrides the
`ProjectionRetry` context reference, whose default is jittered exponential
backoff from 200 ms, capped at 30 s. `run` does not retry. Handler errors retain
their types; `ProjectionFault` carries `phase`, `reason`, and `cause`.

Records are keyed by `id`, `version`, `generation`, and canonical parameters.
Use a new generation for changed input identity or state schema. Retain input
history after the checkpoint, including pinned ranges. Stop the follower first, or use a new generation before
`Projection.forget(bound)`, which calls the Commit's `remove`. For producer
outputs, delete output streams first or use a new generation.

Without a follower, each `current` replays from the last checkpoint or from
the start. A continuously growing input can delay it. Fold handlers must be
replayable and give the same result for any page split.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## License

MIT
