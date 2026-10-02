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
import { Memory, Streamsy } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";

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
const host = ProjectionMemory.layer.pipe(
  Layer.provideMerge(Memory.layer({}, Streamsy.group(Numbers, Doubled))),
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
same positions; handlers must be deterministic. Foreign outputs must already
exist. Same-owner outputs can be created automatically. There is no atomicity
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
`Projection.decide(bound, { input, decide })` runs to the tail, reads, decides,
and appends to one input with that input's offset as `expectedOffset`. It
retries on `OffsetMismatch`. See [Read, decide, append](https://streamsy.dev/docs/projections/state#read-decide-append).

`Serve.value(schema, effect)` serves values computed with `Projection.read`.
Before the first saved unit, reads return the declared initial value.
`Serve.mount(Board, path)` and `Serve.mount(Transitions, path)` mount output routes.

Outputs accept routes or fixed addresses, like inputs. `Streamsy.json` accepts
items. `Streamsy.state` accepts `DurableState.upsert("card", row)` and
`DurableState.remove("card", key)` from `@streamsy/core`. Each change names its
collection. Two outputs cannot name the same stream or an input.

## Hosts

| Layer                                     | Provides                            | Requires                              |
| ----------------------------------------- | ----------------------------------- | ------------------------------------- |
| `ProjectionSql.layerStorage` from `./sql` | `CheckpointStore`, `Transaction`    | Storage `CommitBoundary`, `SqlClient` |
| `ProjectionSql.layer` from `./sql`        | `CheckpointStore`, `Transaction`    | Plain `SqlClient`                     |
| `ProjectionMemory.layer` from `./memory`  | `CheckpointStore`                   | Memory storage boundary               |
| `ProjectionMemory.layerMemory()`          | `CheckpointStore`, protocol streams | Nothing                               |
| `CheckpointStore.layerMemory`             | Independent `CheckpointStore`       | Nothing                               |

Compose the projection Layer over storage with `Layer.provideMerge` and retain
one Layer graph for the host lifetime. SQL uses `streamsy_projection_v2_records`.
Old v1 projection tables are not migrated or read; projections rebuild from zero.
Layer authors use `EncodedStore` and `fromStore` from `@streamsy/projection/checkpoint`.

## Run and recover

`declaration.at(params)` binds a projection. `pass` runs one unit; `run` continues
until caught up, closed, or its optional unit limit. `follow` returns a scoped
fiber and waits for changes; `watch` needs storage change feeds. Remote inputs
work with `follow` and `run`.

`follow` retries only `storage-failure`: `options.retry` overrides the
`ProjectionRetry` context reference, whose default is jittered exponential
backoff from 200 ms, capped at 30 s. `run` does not retry. Handler errors retain
their types; `ProjectionFault` carries `phase`, `reason`, and `cause`.

Records are keyed by `id`, `version`, `generation`, and canonical parameters.
Use a new generation for changed input identity or state schema. Retain input
history after the checkpoint, including pinned ranges. Stop runners before
`Projection.forget(bound)`, which calls the Commit's `remove`. For producer
outputs, delete output streams first or use a new generation.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## License

MIT
