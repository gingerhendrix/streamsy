# @streamsy/serve

Effect HttpRouter routes for streams, Durable State and JSON documents, plus Bun
and Cloudflare host glue and durable action delivery. Requires Effect
`4.0.0-rc.115`.

```ts
import { Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { BunRuntime } from "@effect/platform-bun";
import { Http, StreamRoute, Streams } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";

const events = StreamRoute.json("events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
});
const App = Layer.mergeAll(
  Http.routes({ prefix: "/streams" }),
  Serve.stream(events, "/feed/:seat"),
);
BunRuntime.runMain(
  Layer.launch(
    HttpRouter.serve(App).pipe(
      Layer.provide(Streams.layerMemory()),
      Layer.provide(listener({ port: 3000 })),
    ),
  ),
);
```

`Serve.stream` reads a family stream through `StreamsReader`. `Serve.state`
requires a StateRef family and reads its Durable State changes from `-1`, with
no snapshot. `Serve.document` takes a value source with a Schema and a
`resolve(params)` Effect; it serves canonical JSON with a deterministic ETag.
Path parameters decode with the family's original Schema codecs. An explicit
`params` Effect on stream/state can instead read a service provided by
`HttpRouter.middleware`. Auth and on-demand projection runs are application
middleware.

Every Serve path owns all methods: GET and HEAD read; others return 405 with
`allow: GET, HEAD`, unless CORS middleware answers a preflight. A Serve path
beneath the protocol prefix is not a second writable stream id. Duplicate
route shapes fail application construction. Protect the protocol routes too
when their underlying data is private.

Bun's listener defaults to `idleTimeout: 0`. Route handlers are interruptible
on the pinned Effect/Bun router; client abort releases a parked long poll.
The caller owns the Layer scope. Tests or custom hosts can use Effect's
`HttpRouter.toWebHandler(App)` and must call its `dispose` at shutdown.

| Entry                                                                   | Exports                                                                           |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `.`                                                                     | `Serve.stream`, `Serve.state`, `Serve.document`, their types; `PublicErrorSchema` |
| `/contract`                                                             | Effect-free headers, public errors/decoder, canonical JSON, fingerprints          |
| `/bun`                                                                  | `listener`, listener options and defaults                                         |
| `/cloudflare`                                                           | `StreamsyObject`, placement router/rules, alarm service                           |
| `/alchemy`                                                              | `objectHandlers`, placement router/rules, alarm service                           |
| `/action`                                                               | Unchanged action declarations                                                     |
| `/action/errors`, `/action/outbox`, `/action/runtime`, `/action/sqlite` | Unchanged action delivery and backing APIs, except unused OUTBOX_SCHEMA removed   |

Ten physical entries form six groups. Action's existing Sink names remain.
State/stream recovery is `replay-from-start`; state consumers clear their local
collection before replay. Old sink headers and snapshot reset exports are gone.
Fingerprints cover route/member metadata and an optional `contract` revision;
change that revision when codec behavior changes without metadata changing.
Fingerprints are change detectors, not authorization or cryptographic identities.

Guides: [Serve](https://streamsy.dev/docs/projections/serve),
[HTTP](https://streamsy.dev/docs/runtime/http),
[Cloudflare](https://streamsy.dev/docs/runtime/cloudflare),
[Alchemy](https://streamsy.dev/docs/runtime/alchemy).
