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

const run = Projection.run(projection, { limit: 10 });
```

`input: numbers` is one named input; the handler reads `batch.input.items`.
Declare several with `inputs: { orders, refunds }` and read one slice per name.
`Projection.each(handle)` builds a handler that runs once per item.
`Projection.follow(projection, options)` keeps running as new items arrive.
`Projection.family(definition)` declares routed members once, `Projection.serialized`
runs one caller at a time per member key, and `Projection.onChange` runs members
from their same-owner storage change feeds.

[src/examples/minimal.ts](src/examples/minimal.ts) is the complete version of
this example; the package tests execute it.

## Stream output

Fused is for SQL and side effects; stream is the declared-output form.
`Projection.stream` works across backends, including through the fetch Layer.
The output stream is created when absent. Existing outputs are used as they
are; gone or closed outputs fail with `pin / invalid-output`. The handler must
be deterministic on its inputs.

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
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";

const host = ProjectionSqlite.layer.pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: "streams.sqlite" } })),
);
const program = run.pipe(Effect.provide(host));
```

Inside a Durable Object, swap `BunStorage.layerProtocol(...)` for
`DurableObjectStorage.layerProtocol({ client: { storage: ctx.storage } })` from
`@streamsy/storage/durable-object`, and keep one runtime for the object's
lifetime.

A fused handler's writes are exactly-once only when they go through the same
storage as the checkpoint: `Streams.append` on the same Layer, or SQL on the
shared `SqlClient`. Writes anywhere else are at-least-once; use `unit.key` as
an idempotency key for those. The key is stable across pinned stream-form
retries; a fused retry re-reads and may see a longer input range.

## Good to know

- `run` returns `caught-up`, `source-closed`, or `limit-reached` with counts
  of units and items accepted. `limit` counts checkpoint transactions per call;
  the server decides the read page. Without `limit`, a run continues until
  caught up or every input is closed and drained. Each pass reads one page
  per input in declaration order.
- Failures are `ProjectionFault` values with a `phase` and a `reason`. Handler
  errors pass through untouched. Underlying failures are available as `cause`.
  Nothing retries or resets on its own.
- A record is keyed by `id`, `version` (default 1), `generation`, and `params`.
  Version 1 is omitted from the encoded key so existing records keep their key.
  To change inputs or start over, declare a new `generation`; it starts from
  offset zero.
- Input history after the checkpoint must stay readable. Deleting or
  rewriting input streams behind a projection stops it with
  `history-unavailable`.
- SQLite adds one table, `streamsy_projection_v1_records`.

Layer authors use `@streamsy/projection/checkpoint` for the record and store
contracts. A fused projection needs a real owner transaction. The memory
checkpoint Layer must share the stream Layer graph. Input history, including
pending pinned ranges, must remain readable.

`follow` returns a caller-scoped fiber and repairs missed wake hints every
1000 ms by default. It has no default unit cap.

Full reference: [streamsy.dev/docs/projections](https://streamsy.dev/docs/projections).

## License

MIT
