# @streamsy/projection

Checkpointed Effect projections over one or more retained streams.

A projection is a named, typed, checkpointed consumer of protocol batches from
one or more input streams. It processes one unit at a time through an Effect
handler. State is optional and belongs to the services the handler uses.
At-least-once is the baseline; the two shipped adapters give more:

- the **fused** form commits the handler's local writes and the checkpoint in
  one owner transaction;
- the **stream** form appends the handler's output to a stream under a
  producer tuple with a durably pinned unit, so each unit lands once.

The same kernel runs in memory, Bun SQLite and same-object Durable Object
SQLite. Effect is pinned to `4.0.0-rc.115`.

```text
restore → read each input → process one unit → commit checkpoint (and local writes)
```

## Define a projection

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

`Projection.make` is inert and acquires no service. `input: ref` is sugar for
`inputs: { input: ref }`; the handler reads `batch.input.items`. With several
inputs, declare `inputs: { orders, refunds }` and read one slice per name.
Every declared input has a slice in every batch, empty or not. Each slice has
`from`, `items`, `nextOffset`, `upToDate` and `closed`.

The handler receives the batch and a `Unit`: the projection identity, the
non-empty `ranges` keyed by input, and a `key` that is stable across retries
of the same unit. Use `unit.key` as the idempotency key for a target outside
the transaction.

`Projection.items(batch)` flattens a batch into `{ input, item }` entries in
declaration order, then stream order. `Projection.each(handle)` builds a fused
handler that runs `handle(entry, unit)` once per entry, one at a time.

[src/examples/minimal.ts](src/examples/minimal.ts) is typechecked and compiled
with the package and executed in the package tests. It provisions `numbers`
and `doubled`, appends `[1, 2, 3]`, and one run yields `[2, 4, 6]`.

### Stream output

Use `Projection.stream` when the output stream is not reachable from the
checkpoint transaction, for example a stream on another backend or behind a
Fetch client.

```ts
import { Effect } from "effect";

const totals = Projection.stream({
  id: "totals",
  input: numbers,
  output: StreamRef.json("totals", { schema: Schema.Finite }),
  process: (batch) => Effect.succeed([batch.input.items.reduce((sum, n) => sum + n, 0)]),
});
```

The handler returns the items to append. The kernel pins the unit's ranges and
producer sequence in the checkpoint before the append, then appends under a
producer tuple whose epoch is the generation. A retry reproduces the pinned
ranges from the input log and sends the same tuple; `Appended` and `Duplicate`
both settle the unit. The handler must be deterministic on its inputs for
byte-identical retries: the protocol compares tuples, not payloads. See
[CONTRACT.md](CONTRACT.md) for the retry rules.

## Compose one host

Memory has a convenience graph that retains the real Streams memory owner:

```ts
import { layerMemory } from "@streamsy/projection/memory";
const program = run.pipe(Effect.provide(layerMemory()));
```

For SQLite, select a driver at the host and pass the same owned client and
boundary to the protocol and the projection Layer. The package root imports
neither SQL driver.

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

Inside one SQLite Durable Object, replace `BunStorage.layer(...)` with
`DurableObjectStorage.layer({ client: { storage: ctx.storage } })`, imported
from `@streamsy/storage/durable-object`. Use one Layer scope or runtime for the
object's lifetime, shared by requests and alarms; do not create a projection
runtime per request. The package's local test harness disposes runtimes to
prove the composition; it is not a serving pattern.

A fused handler's writes join the checkpoint transaction only when they go
through the same owner: `Streams.append` to a stream on the same storage, or
SQL through the shared `SqlClient`. Writes to a separately acquired memory
Layer, a Fetch writer, a foreign SQL client, or any remote target are
at-least-once. Inputs may live anywhere the reader can reach, including a
routed graph where the input is in memory and the checkpoint and output are in
SQLite. `Checkpoints` owns `load`, `save` and `withTransaction`; both shipped
Layers have a real owner transaction, and a store without one must not host a
fused projection.

## Progress and recovery

`pass` restores the record, reads every input once in declaration order with
the item budget carried forward, then processes and checkpoints one unit. In
the fused form the record token is checked again inside the transaction, so an
overlapping runner with stale progress stops with `token-conflict` and nothing
is written. A handler failure escapes the transaction and rolls back every
write inside it, including the checkpoint.

`run` repeats passes with positive safe-integer budgets. Defaults are 100
units per run and 1000 items per pass; an optional `bytes` budget counts stored
payload bytes per pass. A slice that exceeds the remaining bytes is refused
whole: when nothing else was read the pass returns `limit-reached` without a
write, and when an earlier input already contributed the pass commits that
input and leaves the refused one at its offset. Resume with a sufficient
budget. Counters describe accepted work, not all I/O used to read a unit.

Results expose `status`, `units`, `items`, `bytes` and `record`. A non-empty
pass reports `progress`; only an empty pass reports `caught-up`, or
`source-closed` once every input is closed and drained. `run` returns
`caught-up`, `source-closed` or `limit-reached`; available work is drained
before `source-closed`. Empty passes create no record. A unit whose handler
writes nothing still advances the checkpoint. The record holds the accepted
offset per input, and for the stream form the pending pin and producer
sequence.

`follow(projection, { ...budget, repairIntervalMs: 1000 })` returns a
caller-scoped fiber. Join it to observe closure or a typed failure; scope
interruption releases the parked waits and the fiber. Each cycle runs to
completion, then races one `readNext` hint per input, each bounded by the
repair interval; the first response wins, is discarded, and the next `run` is
authoritative. A missed process-local wake delays progress without losing it.
A budget that cannot fit even one unit stays parked between repair passes
until the caller restarts it with a larger budget. On HTTP backends each hint
is one long poll per input.

SQLite reopening resumes from the persisted record and skips accepted input.
Memory restart means a new run on the same live host; memory is not
persistent.

## Identity and generations

A record is keyed by `id`, `generation` and canonical `params`. Its identity
stores the stream id and content type of every declared input by name, so a
change of input name, stream id or content type is detected at `load` as
`identity-mismatch`. A codec change that keeps the same content type is not
detected. There is no reset and no active-generation pointer: declare any such
change under a new generation, which starts from zero offsets and, for the
stream form, a new producer epoch. A stream-form runner of an older generation fails
`stale-epoch` once a newer one has appended. The producer id is `id` alone, or
`id/{canonical params JSON}` when parameterised.

## Faults

Expected failures are `ProjectionFault` values with a `phase` and a `reason`.
The phase says where the run stopped: `load` (record, identity, budget),
`read` (inputs), `pin` (the pinned save, reproduction and the producer
append), `process` (output encoding and adapter failures while the handler
ran) and `checkpoint` (the save after processing completed). Reasons are
`history-unavailable`, `identity-mismatch`, `invalid-record`,
`invalid-source`, `invalid-budget`, `invalid-output`, `token-conflict`,
`range-unreproducible`, `stale-epoch`, `storage-failure` and
`unsupported-composition`; input faults name the `input`. Handler errors are
not wrapped: `run` fails with `E | ProjectionFault`. No implicit retry or reset
occurs. Failures must escape the outer transaction to roll back.

## Retention and storage

Retaining all required input history is a deployment precondition: every
offset after an accepted one, and every pending pinned range, must still be
readable. Missing or deleted streams, a cursor beyond the retained tail, an
empty read that skips progress, or a pinned range that reproduces a different
count stop with `history-unavailable` or `range-unreproducible`. Do not delete
and recreate stream ids or alter retained history behind the protocol; the
protocol has no stream incarnation token, so such mutations cannot all be
detected.

SQLite creates only `streamsy_projection_v1_records(key TEXT PRIMARY KEY NOT
NULL, value TEXT NOT NULL)` in the owned transaction at Layer acquisition.
Keys are encoded tuples of the format namespace, id, generation and canonical
params. The value is a Schema-checked envelope holding the record and its
version; the version is the compare-and-set token. This additive
package-owned table is never reset or repurposed.

Memory stores the same envelopes in the fused record map of the Streams memory
owner. Use bounded histories; no performance claim is made here.

## Not in this cut

An ordering combinator across inputs, consistent cuts across inputs, a Durable
State helper, an unpinned append mode, owner fencing, and remote fused targets
are outside the package. Inputs are read one after another with no
cross-stream cut guarantee.
