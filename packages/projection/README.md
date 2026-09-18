# @streamsy/projection

Checkpointed Effect projections over one or more Durable Streams. Version
0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/projection @streamsy/core effect
```

A projection reads batches from its input streams, hands one unit at a time to
your handler, and commits a checkpoint after each unit. On restart it resumes
from the checkpoint. Two forms are shipped:

- **fused**: the handler's local writes and the checkpoint commit in one
  transaction, so each unit is applied exactly once;
- **stream**: the handler returns items, and the kernel appends them to an
  output stream under a producer tuple, so a retry lands the same unit once.

The same kernel runs in memory, in Bun SQLite, and inside a SQLite Durable
Object.

## Define and run

```ts
import { Schema } from "effect";
import { StreamRef, Streams } from "@streamsy/core";
import { Projection } from "@streamsy/projection";

const numbers = StreamRef.json("numbers", { schema: Schema.Finite });
const doubled = StreamRef.json("doubled", { schema: Schema.Finite });

const projection = Projection.make({
  id: "doubled",
  input: numbers,
  process: (batch) =>
    Streams.append(
      doubled,
      batch.input.items.map((n) => n * 2),
    ),
});

const run = Projection.run(projection, { units: 10, items: 100 });
```

`input: numbers` is one named input; the handler reads `batch.input.items`.
Declare several with `inputs: { orders, refunds }` and read one slice per name.
`Projection.each(handle)` builds a handler that runs once per item.
`Projection.follow(projection, options)` keeps running as new items arrive.

[src/examples/minimal.ts](src/examples/minimal.ts) is the complete version of
this example; the package tests execute it.

## Stream output

Use `Projection.stream` when the output stream is not reachable from the
checkpoint transaction, for example a stream on another backend or behind the
fetch Layer. The handler must be deterministic on its inputs.

```ts
import { Effect } from "effect";

const totals = Projection.stream({
  id: "totals",
  input: numbers,
  output: StreamRef.json("totals", { schema: Schema.Finite }),
  process: (batch) => Effect.succeed([batch.input.items.reduce((sum, n) => sum + n, 0)]),
});
```

## Choose a host

In memory:

```ts
import { layerMemory } from "@streamsy/projection/memory";
const program = run.pipe(Effect.provide(layerMemory()));
```

On Bun SQLite, share one storage Layer between the protocol and the projection:

```ts
import { Layer } from "effect";
import { Protocol } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";

const host = Layer.merge(Protocol.layer(), ProjectionSqlite.layer).pipe(
  Layer.provideMerge(BunStorage.layer({ client: { filename: "streams.sqlite" } })),
);
const program = run.pipe(Effect.provide(host));
```

Inside a Durable Object, swap `BunStorage.layer(...)` for
`DurableObjectStorage.layer({ client: { storage: ctx.storage } })` from
`@streamsy/storage/durable-object`, and keep one runtime for the object's
lifetime.

A fused handler's writes are exactly-once only when they go through the same
storage as the checkpoint: `Streams.append` on the same Layer, or SQL on the
shared `SqlClient`. Writes anywhere else are at-least-once; use `unit.key` as
an idempotency key for those.

## Good to know

- `run` returns `caught-up`, `source-closed`, or `limit-reached` with counts
  of units, items, and bytes accepted. Budgets are per run (`units`) and per
  pass (`items`, `bytes`).
- Failures are `ProjectionFault` values with a `phase` and a `reason`. Handler
  errors pass through untouched. Nothing retries or resets on its own.
- A record is keyed by `id`, `generation`, and `params`. To change inputs or
  start over, declare a new `generation`; it starts from offset zero.
- Input history after the checkpoint must stay readable. Deleting or
  rewriting input streams behind a projection stops it with
  `history-unavailable`.
- SQLite adds one table, `streamsy_projection_v1_records`.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## License

MIT
