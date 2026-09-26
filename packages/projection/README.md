# @streamsy/projection

Checkpointed Effect projections over one or more Durable Streams. Version
0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/projection @streamsy/core effect
```

A projection reads batches from its input streams, hands one unit at a time to
your handler, and commits a checkpoint after each unit. On restart it resumes
from the checkpoint. Two forms are shipped:

- **atomic**: the handler's local writes and the checkpoint commit in one
  transaction, so each unit is applied exactly once;
- **outputs**: the handler returns items, and the kernel appends them to
  output streams under a producer tuple, so a retry lands the same unit once.

The same kernel runs in memory, in Bun SQLite, and inside a SQLite Durable
Object.

## Define and run

```ts
import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";

class Numbers extends Streamsy.json<Numbers>()("Readme/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}
class Doubled extends Streamsy.json<Doubled>()("Readme/Doubled", "doubled", {
  params: {},
  schema: Schema.Finite,
}) {}

const projection = Projection.atomic({
  id: "doubled",
  params: {},
  inputs: { input: Numbers },
  process: (batch) =>
    Effect.flatMap(Doubled, (output) =>
      output.append(
        {},
        batch.input.items.map((n) => n * 2),
      ),
    ),
});

const run = Projection.run(projection.at({}), { limit: 10 });
```

`inputs: { input: Numbers }` declares one named input; the handler reads `batch.input.items`.
Declare several with `inputs: { orders: Orders, refunds: Refunds }` and read one slice per name.

Every constructor takes `params` codecs and returns a declaration. `projection.at(params)`
binds it, and runners take the bound value. An input or output that names a stream
declaration is resolved with the same params. A fixed address is used as it is.
A projection with no parameters uses `params: {}` and `.at({})`. The handler reads
the decoded values from `unit.params`. See
[Parameters](https://streamsy.dev/docs/projections/projections#parameters).

`Projection.each(handle)` builds a handler that runs once per item.
`Projection.follow(projection, options)` keeps running as new items arrive.
`Projection.run(projection, options)` runs one caller at a time per canonical key;
see [Triggers](https://streamsy.dev/docs/projections/projections#triggers).
`Projection.watch` runs a bound projection from same-owner storage change feeds; see
[Triggers](https://streamsy.dev/docs/projections/projections#triggers).

[src/examples/minimal.ts](src/examples/minimal.ts) is the complete version of
this example; the package tests execute it.

## State

Both forms take `state: { schema, initial }`. `initial` is a value, or a function of
the decoded params, used only when no state is saved yet. The state is saved in the
same transaction as the checkpoint.

An atomic projection with `state:` gives a `fold` step in place of `process`.
The unit loads the state once, folds each item in order, and saves it once:

```ts
const total = Projection.atomic({
  id: "running-total",
  params: {},
  inputs: { input: Numbers },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, { item }) => sum + item,
}).at({});

const program = Effect.gen(function* () {
  yield* Projection.run(total);
  const { state, offsets } = yield* Projection.read(total);
});
```

`Projection.read(projection)` returns `{ state, offsets }`: the typed state and the
offset of each input that it is at, read in one transaction. `Progress.offsets` from
`run` and `pass` gives the same offsets.
`Projection.forget(projection)` takes the serialized permit, deletes state and checkpoint rows,
and retires the process lock after its existing callers drain.

The state is one value, and each unit rewrites all of it. Keep it small: a counter,
a summary, or up to a few hundred rows. Put large row sets in application tables
through the shared `SqlClient` in an atomic projection. Keyed state is planned.
See [State](https://streamsy.dev/docs/projections/projections#state) for transaction,
size, generation, and retirement rules.

## Read, decide, append

To validate a command against current state, run the projection, read it, decide,
and append to its input with `expectedOffset: offsets.input`. If another writer
moved the tail, the append writes nothing and fails with `OffsetMismatch`. Retry
the whole step a bounded number of times:

```ts
const command = Effect.gen(function* () {
  yield* Projection.run(total);
  const { state, offsets } = yield* Projection.read(total);
  if (state >= 100) return "full";
  const numbers = yield* Numbers;
  yield* numbers.append({}, [1], { expectedOffset: offsets.input });
  return "added";
}).pipe(Effect.retry({ while: (error) => error._tag === "OffsetMismatch", times: 5 }));
```

See [Read, decide, append](https://streamsy.dev/docs/projections/projections#read-decide-append).

## Outputs

`Projection.outputs({ id, params, inputs, outputs, state?, process })` declares pinned
outputs. Import `Output` from the package root:

- `Output.stream(schema, { stream })` appends typed JSON items. The option is an id
  or `{ readonly id: string }`; only the id is used, and the declared schema owns encoding.
- `Output.rows(schema, { key, stream })` appends `Output.upsert(row)` and
  `Output.remove(key)` changes. The output name is the collection type; the kernel owns no rows table.

```ts
import { Output } from "@streamsy/projection";

class Totals extends Streamsy.json<Totals>()("Readme/Totals", "totals", {
  params: {},
  schema: Schema.Finite,
}) {}

const totals = Projection.outputs({
  id: "totals",
  params: {},
  inputs: { input: Numbers },
  outputs: { output: Output.stream(Schema.Finite, { stream: Totals }) },
  process: (batch) =>
    Effect.succeed({ output: [batch.input.items.reduce((sum, n) => sum + n, 0)] }),
});
```

The handler returns an Effect of `{ <outputName>: [...] }`, with every
output present (use `[]` to skip one). With `state:`, the handler is
`(state, batch, unit)`: it gets the prior state first and returns the next state
in the reserved `state` result key. `state` cannot name an output.
`Projection.each` concatenates keyed arrays returned by each item's Effect.
The declaration's `state` field is a source for `Serve.value`.

The output streams are created when absent. Existing outputs are used as they
are; gone or closed outputs fail with `pin / invalid-output`.
Each output has its own sequence. Recovery reprocesses the pinned inputs from
the previous state and resends all pinned outputs; state and checkpoint commit
together at settle. Processing must be deterministic. There is no atomicity
across outputs. Two outputs cannot share a stream or name one of the projection's inputs.

Producer ids are `id/v<version>[/canonicalParamsJson]`, including `/v1`.
A new version or generation should write to a new event stream.
See [Outputs](https://streamsy.dev/docs/projections/projections#outputs) for a complete declaration.

## Choose a host

In memory, build the projection services over the memory stream Layer:

```ts
import { Layer } from "effect";
import { Memory, Streamsy } from "@streamsy/core";
import * as ProjectionMemory from "@streamsy/projection/memory";

const streams = Streamsy.group(Numbers, Doubled);
const host = ProjectionMemory.layer.pipe(Layer.provideMerge(Memory.layer({}, streams)));
const program = run.pipe(Effect.provide(host));
```

On Bun SQLite, share one storage Layer between the streams and the projection:

```ts
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";

const host = ProjectionSqlite.layer.pipe(
  Layer.provideMerge(BunStorage.layer({ client: { filename: "streams.sqlite" } }, streams)),
);
const program = run.pipe(Effect.provide(host));
```

Inside a Durable Object, swap `BunStorage.layer(...)` for
`DurableObjectStorage.layer({ client: { storage: ctx.storage } }, streams)` from
`@streamsy/storage/durable-object`, and keep one runtime for the object's
lifetime.

## Guarantees

| Effect                                                                                           | Guarantee                                                 |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------------- |
| SQL on the shared `SqlClient` and stream appends on the checkpoint owner, in `Projection.atomic` | Exactly once, with the checkpoint                         |
| The `state:` value, in either form                                                               | Exactly once, with the checkpoint                         |
| Outputs on the checkpoint owner, in `Projection.outputs`                                         | Effectively once, through the producer tuple              |
| An output or atomic stream write on another storage owner                                        | Refused with `unsupported-composition` before it runs     |
| A call to another system inside the handler                                                      | At least once. A rollback or a recovery repeats the call. |

For calls to another system, use `unit.key` as an idempotency key. The key is
stable across pinned outputs retries; an atomic retry re-reads and may see a
longer input range. See
[Guarantees](https://streamsy.dev/docs/projections/projections#guarantees).

## Good to know

- `run` returns `caught-up`, `source-closed`, or `limit-reached` with counts
  of units and items accepted. `limit` counts checkpoint transactions per call;
  the server decides the read page. Without `limit`, a run continues until
  caught up or every input is closed and drained. Each pass reads one page
  per input in declaration order.
- Failures are `ProjectionFault` values with a `phase` and a `reason`. Handler
  errors pass through untouched. Underlying failures are available as `cause`.
  `run` is single-shot; `follow` retries `storage-failure` with logged backoff.
- A record is keyed by `id`, `version` (default 1), `generation`, and `params`.
  Version 1 is omitted from the encoded checkpoint key; producer ids always include it.
  To change inputs or start over, declare a new `generation`; it starts from
  offset zero.
- Input history after the checkpoint must stay readable. Deleting or
  rewriting input streams behind a projection stops it with
  `history-unavailable`.
- SQLite adds `streamsy_projection_v1_records` and `streamsy_projection_v1_state`.

Layer authors use `@streamsy/projection/checkpoint` for the record and store
contracts. An atomic projection needs a real owner transaction. The memory
checkpoint Layer must share the stream Layer graph. Input history, including
pending pinned ranges, must remain readable.

`follow` returns a caller-scoped fiber and repairs missed wake hints every
1000 ms by default. It has no default unit cap. Storage failures retry forever with
jittered exponential backoff from 200 ms, capped at 30 s; `retry` accepts a `Schedule` override.
Each successful cycle resets the schedule; a finite budget counts consecutive failed cycles.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## Migrating from 0.4.0

| 0.4.0                                               | Now                                                                                                  |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| The `fused` constructor                             | `Projection.atomic`                                                                                  |
| `Projection.stream`                                 | `Projection.outputs` with one `Output.stream`                                                        |
| `loadState(projection, schema)`                     | `Projection.read(projection)`, which returns `{ state, offsets }`                                    |
| `record` on `Progress` (`record.inputs`)            | `offsets` on `Progress`                                                                              |
| The two-argument `fold(initial, step)` in `process` | `state: { schema, initial }` and `fold: (state, entry, unit) => next` on the declaration             |
| A `value` output in `outputs`                       | `state: { schema, initial }`, and the handler `(state, batch, unit)` returns `{ state, ...outputs }` |
| `Serve.value` on a bound `value` output             | `Serve.value(declaration.state, path)`                                                               |

In `@streamsy/serve`, the value contract header is now `x-streamsy-value-contract`
(`VALUE_CONTRACT_HEADER`), and its options type is `ValueOptions`. The old names
used `document` in place of `value`. Update CORS `allowedHeaders` and
`exposedHeaders`. A client that sends a fingerprint kept from 0.4.0 gets one 409
`ContractChanged` and refetches.

**Warning.** A checkpoint written by `Projection.stream` keeps its producer
position under the output name `stream`. Suppose you move that projection to
`Projection.outputs` with the same `id`, `version` and `generation`, and name
the output something else, such as `output`. The new output restarts its
producer sequence at 0 under the same producer id, and the server drops its
appends as duplicates, with no error. Name the output `stream`, or declare a
new `generation`.

## License

MIT
