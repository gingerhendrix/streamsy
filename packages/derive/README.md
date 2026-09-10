# @streamsy/derive

Derive turns one retained source into one dedicated sink using a pure stateful
step. Each accepted source boundary commits sink output, current state and source
progress together. The same kernel runs in memory, Bun SQLite and same-object
Durable Object SQLite. Effect is pinned to `4.0.0-rc.112`.

```text
restore → pull from Source → pure step → fused commit to Sink
```

## Define a projection

```ts
import { Effect, Schema } from "effect";
import { StreamRef } from "@streamsy/core";
import { Projection, StreamSource, StreamSink } from "@streamsy/derive";

const input = StreamRef.json("numbers", { schema: Schema.Finite });
const output = StreamRef.json("running-totals", { schema: Schema.Finite });

const run = Effect.gen(function* () {
  const projection = Projection.make({
    id: "running-total",
    version: 1,
    source: yield* StreamSource.make(input),
    sink: yield* StreamSink.make(output),
    initial: 0,
    stateSchema: Schema.fromJsonString(Schema.Finite),
    step: (state, item) => ({ state: state + item, outputs: [state + item] }),
  });
  return yield* Projection.catchUp(projection, { boundaries: 100, items: 1000 });
});
```

Provision both streams first. The sink must initially be empty, dedicated to the
projection, and have the ref's content type. The step must be deterministic and
pure, including not mutating the initial state, source items or prior state. It
receives a context with projection identity and the exact boundary end position.
It has no services, clock or runtime authority. Stateless mapping uses unit state.
Codecs have no service requirements; provide persisted state as a string Codec,
normally `Schema.fromJsonString(yourSchema)`.

[src/examples/minimal.ts](src/examples/minimal.ts) is typechecked and compiled with the
package and executed in the package tests. It provisions streams and maps
`[1, 2, 3]` to cumulative totals `[1, 3, 6]`.

## Compose one host

Memory has a convenience graph that retains the real Streams memory owner:

```ts
import { layerMemory } from "@streamsy/derive/memory";
const program = run.pipe(Effect.provide(layerMemory()));
```

For SQLite, select a driver at the host and pass the same owned client and
boundary to protocol and Derive. The Derive root imports neither SQL driver.

```ts
import { Layer } from "effect";
import { Protocol } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import * as DeriveSqlite from "@streamsy/derive/sqlite";

const host = Layer.merge(Protocol.layer(), DeriveSqlite.layer).pipe(
  Layer.provideMerge(BunStorage.layer({ client: { filename: "streams.sqlite" } })),
);
const program = run.pipe(Effect.provide(host));
```

Inside one SQLite Durable Object, replace `BunStorage.layer(...)` with
`DurableObjectStorage.layer({ client: { storage: ctx.storage } })`, imported from
`@streamsy/storage/durable-object`. Use one Layer scope/runtime for the object's
lifetime, shared by requests and alarms. Do not create a projection runtime per
request. The local test harness disposes runtimes to prove the composition; it
is not a serving pattern.

Do not combine separately acquired memory Layers, a Fetch writer, foreign SQL
clients, or independently owned sinks. `Commit` owns `checkpoints`, `states` and
`withTransaction`; a `Sink` carries that exact owner. Custom sinks must perform
only writes joining this boundary, on its owner fiber. Custom store/sink authors
are responsible for honoring this capability contract. Matching the Commit object
alone cannot prove an arbitrary implementation's I/O is atomic. No remote sink
or multi-owner composition is supported. One runner per projection id is required.

## Progress and recovery

`pass` restores checkpoint/state, pulls once, steps in order, encodes state, then
rechecks the checkpoint inside a fused transaction before writing the sink,
state, and checkpoint. An overlapping pass with stale progress stops with a
sink conflict. This is a local misuse check, not owner fencing.

`catchUp` repeats passes with positive safe-integer limits. Defaults are 100
boundaries and 1000 items; an optional `bytes` limit counts source-reported bytes.
The stream adapter reports stored message payload bytes. Unreported bytes count
as zero; generic Sources without byte reporting cannot promise a byte bound.
Counters describe accepted work, not all I/O or memory used to read a boundary.
Whole boundaries are never split by the kernel. An oversized boundary returns
`limit-reached` without committing it; resume with a sufficient budget. Custom
Sources return `_tag: "Boundary"` or `_tag: "LimitReached"` and fail through the
`DeriveFault` channel when history is unavailable. They should respect the requested item bound. Empty-output steps still save
state and checkpoint while leaving the sink position unchanged. Empty caught-up
reads do not create records or increment revisions.

Results expose `status`, `boundaries`, `items`, `bytes`, and `checkpoint`.
`pass` may return `progress`; `catchUp` returns `caught-up`, `limit-reached` or
`source-closed`. Available work is drained before source-closed. The checkpoint
is the accepted source position, resulting sink position and matching state
revision. An initial revision-zero checkpoint is a logical starting position,
not evidence of a stored commit.

`follow(projection, { ...limits, repairIntervalMs: 1000 })` returns a caller-scoped
fiber. Join it to observe source closure or typed failure. Scope interruption
releases waits and the fiber. It catches up, then uses Source.wait as a hint and
runs another bounded pass after a wake or repair timeout. The stream wait uses
direct readNext; its response is discarded and authoritative bounded pull drives
progress. A missed process-local wake delays progress without losing it. A budget
that cannot fit even one boundary stays parked between repair passes until the
caller cancels and restarts with a larger budget.

SQLite reopening resumes from persisted state and skips accepted input. Memory
restart means a new projection on the same live host; memory is not persistent.
Identity includes id, stringified version, `generation = v{version}`, source and
sink identities. Numeric `1` and string `"1"` deliberately denote the same version.
Stream identities encode id and content type; codec changes require a version
change. Stores key by projection id, so changing version, generation, source or
sink stops. There is no reset or active-generation pointer. Start a new id and
sink for a fresh lane.

Expected failures use `DeriveFault.reason`: `history-unavailable`,
`identity-mismatch`, `invalid-state`, `sink-conflict`, `storage-failure`,
`unsupported-composition`, `invalid-source`, or `invalid-limits`. No implicit retry
or reset occurs. Schema failures and mismatched/missing paired records stop.
A failing commit discards all three writes. Failures must escape the outer
transaction to roll back; nested transactions have no savepoints. Protocol
errors are mapped to `DeriveFault` by the stream adapters. `OffsetMismatch` becomes
`sink-conflict` with expected and actual offsets. Missing/gone source streams and
source offset regression become `history-unavailable`; infrastructure failures remain
`storage-failure`. These errors escape the transaction before recovery.

## Retention and storage

Retaining all required source history is a deployment precondition. Missing or
deleted streams, a cursor beyond the retained tail, or an empty read that skips
progress stop as history-unavailable. Do not delete/recreate stream ids or alter
retained message history behind the protocol. The direct protocol has no stream
incarnation token or arbitrary gap certificate, so those unsupported mutations
cannot all be detected. No serving surface claims `syncedThrough`.

SQLite creates only `streamsy_derive_v1_records(key TEXT PRIMARY KEY NOT NULL,
value TEXT NOT NULL)` in the owned transaction. Keys are encoded tuples containing
format namespace, record kind, and projection id. The state envelope stores its
revision and application-encoded string; checkpoint envelopes include identity
and both positions. Both envelopes and application state are Schema checked.
This additive package-owned v1 migration uses CREATE TABLE IF NOT EXISTS; it does
not modify stream tables or import the historical views store. Future format
changes need an explicit migration decision, not reuse of this table's meaning.

Memory stores encoded strings in Slice A's fused record map. Each outer memory
mutation copies the retained host state, including standalone stream mutations.
Use bounded histories; no performance improvement or benchmark is claimed here.

## Deferred

Snapshots, replay-safe cross-resource Commit, at-least-once/deduplicated sink
modes, pending writes, fencing, overlapping-owner handover, online generation
activation, routing, multiple sources, richer lineage, views-store absorption,
outbox/external side effects, causal serving coverage, Fetch integration,
Transact, demo migration, releases and hosted deployment remain outside Step 5.
