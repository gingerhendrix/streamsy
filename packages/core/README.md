# @streamsy/core

Effect-first Durable Streams protocol, typed toolkit, storage contract and
in-process memory Layer. Version 0.4.0 requires `effect@4.0.0-rc.112`.

```ts
import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.String });
const program = Effect.gen(function* () {
  yield* Streams.create(events);
  const appended = yield* Streams.append(events, ["hello"]);
  if (appended.status !== "appended") return appended;
  return yield* Streams.read(events).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
```

Compiled source: `packages/core/test/readme.ts` (checked by `site:validate`).

Applications own the runtime and Layer lifetime. Memory is nonpersistent and
process-local. Persistent protocol storage and an Effect fetch transport remain
later work; a successful memory run proves neither cross-process persistence nor
hosted support.

| Entry                    | Surface                                                                                       |
| ------------------------ | --------------------------------------------------------------------------------------------- |
| `@streamsy/core`         | Schema values, protocol tags/outcomes, Streams, StreamRef, Fold, Producer, Memory and Storage |
| `@streamsy/core/storage` | Storage contract, capabilities and mutation model                                             |
| `@streamsy/core/http`    | `makeEdge` Web conversion; owner must dispose the edge                                        |
| `@streamsy/core/testing` | Bun contract registration, fault injection and test Layers                                    |

`@streamsy/serve/bun` owns a Bun listener and HTTP edge together. The ordinary
core, HTTP and storage entries do not load the Bun test runner. Internal offset
helpers and implementation modules have no public subpaths.

See `docs/api.md` and `docs/storage-contract.md` in the corresponding source
checkout for this version. Source: https://github.com/gingerhendrix/streamsy.

Licensed under MIT; see LICENSE in this package.
