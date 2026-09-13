# @streamsy/core

Effect-first Durable Streams protocol, typed toolkit, storage contract and
in-process memory Layer. Version 0.4.0 requires `effect@4.0.0-rc.115`.

```ts
import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.String });
const program = Effect.gen(function* () {
  yield* Streams.create(events);

  yield* Streams.append(events, ["hello"]);

  return yield* Streams.read(events).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
```

Compiled source: [packages/core/test/readme.ts](https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/packages/core/test/readme.ts).

The example is checked by `site:validate`.

Create and append return only `Created` / `Exists` and `Appended` / `Duplicate`
variants, discriminated by `_tag`. Reads and head return their successful data;
remove returns `void`. Long polls add `timedOut`, and `closed` is always boolean.
Protocol failures carry `id` and are handled with `Effect.catchTag`, including
`StreamNotFound`, `StreamGone`, `OffsetMismatch`, and `AppendConflict`. Codec errors
remain `EncodeFault` / `DecodeFault`; direct infrastructure failures use `StorageFault`.

Applications own the runtime and Layer lifetime. Memory is nonpersistent and
process-local. `@streamsy/storage/bun` provides retained-file Bun SQLite protocol
storage. The Effect fetch Layer is available; hosted Durable Object execution
and release acceptance remain unverified. A successful memory run proves neither cross-process persistence nor hosted
support.

`StreamRoute` declares an id family and constructs its typed refs.
`Backend.make(name)` re-tags one complete protocol graph, and
`Streams.layerRouted(bindings)` supplies one reader/writer pair that selects a
backend per id. Route templates in one binding table must not overlap, and a
fork must remain within one backend.

| Entry                    | Surface                                                                                                                        |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `@streamsy/core`         | Schema values, protocol tags, results and errors, Streams, StreamRef, StreamRoute, Backend, Fold, Producer, Memory and Storage |
| `@streamsy/core/storage` | Storage contract, capabilities and mutation model                                                                              |
| `@streamsy/core/http`    | `app` plus `makeEdge` Web conversion; owner must dispose the edge                                                              |
| `@streamsy/core/testing` | Bun contract registration, fault injection and test Layers                                                                     |

`@streamsy/serve/bun` owns a Bun listener and HTTP edge together. The ordinary
core, HTTP and storage entries do not load the Bun test runner. Internal offset
helpers and implementation modules have no public subpaths.

See `docs/api.md` and `docs/storage-contract.md` in the corresponding source
checkout for this version. Source: https://github.com/gingerhendrix/streamsy.

Licensed under MIT; see LICENSE in this package.

## Remote access and browsers

Import `@streamsy/core/fetch` as `Fetch` and provide `Fetch.layer({ baseUrl })`
with Effect's `FetchHttpClient.layer` (or another `HttpClient`). It supplies the
existing reader and writer services, including finite reads and one-shot long polls,
over the standard Durable Streams HTTP protocol, so it works against any conformant
host. Read results carry message payloads and batch metadata; per-message offsets and
timestamps stay in storage. Text and binary bodies merge into one payload, and JSON
messages return by value. Protocol rejections use tagged errors; remote infrastructure failures use
`TransportFault`, and interruption cancels requests. Dispose the owning runtime at
shutdown. CAS and producer appends require explicit deployment capability assertions;
unknown support fails with `NotSupported` before sending.

Browsers use the official `@durable-streams/client`, `@durable-streams/state`, and
`@durable-streams/state/db` packages directly with an Effect-free validator.
See the [remote and browser guide](../../site/content/docs/user/remote-browser.mdx)
and the compiled [remote example](test/remote.ts).
