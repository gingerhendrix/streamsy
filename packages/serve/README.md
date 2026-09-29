# @streamsy/serve

Effect HttpRouter routes for streams, Durable State and JSON values, plus Bun
and Cloudflare host glue and durable action delivery. Requires Effect
`4.0.0-rc.115`.

```ts
import { Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { BunRuntime } from "@effect/platform-bun";
import { Http, Streamsy, Memory } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { drainOnShutdown, listener } from "@streamsy/serve/bun";

class Events extends Streamsy.json<Events>()("Readme/Events", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}
const streams = Streamsy.group(Events);
const App = Layer.mergeAll(
  Http.routes(streams, { prefix: "/streams", access: "full" }),
  Serve.stream(Events, "/feed/:seat"),
);
BunRuntime.runMain(
  Layer.launch(
    HttpRouter.serve(App).pipe(
      drainOnShutdown(),
      Layer.provide(Memory.layer({}, streams)),
      Layer.provide(listener({ port: 3000 })),
    ),
  ),
);
```

`Serve.stream` reads a stream or bound `Output.stream` through `StreamsReader`.
`Serve.state` accepts a state declaration or bound `Output.rows` and reads changes from `-1`, with
no snapshot. `Serve.value` takes a value source with a Schema and a
`resolve(params)` Effect, including the `state` of a projection declaration; it serves canonical JSON with a deterministic ETag.
Reads check outputs only: an initially absent stream answers 404 and an absent
value answers 503 `ValueUnavailable`. They create no stream, checkpoint or state row.
Path parameters decode with the stream's original Schema codecs. An explicit
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

### Draining live readers on shutdown

Without a drain, closing the serve layer interrupts every handler. A parked
long poll then gets an empty `503` and an open SSE body is reset with no final
event. Wrap the serve layer with `drainOnShutdown()` so live readers take
their normal reconnect path instead:

```ts
HttpRouter.serve(App).pipe(
  drainOnShutdown({ timeout: "2 seconds" }),
  Layer.provide(Memory.layer({}, streams)),
  Layer.provideMerge(listener({ port: 3000 })),
);
```

Apply it to the serve layer itself, before storage and the listener. Layers
close in the reverse order of their build, so the drain closes first. The
shutdown order is then:

1. The drain signals every live read. A parked long poll answers `204` with
   `Stream-Next-Offset`, `Stream-Up-To-Date` and a fresh `Stream-Cursor`, the
   same as a long-poll timeout. An open SSE body ends after its last whole
   batch, which ends with a control event.
2. The drain waits until no live read is running, or until `timeout` passes
   (default 2 seconds). A live request that arrives in this window gets the
   same reply at once.
3. The serve layer closes and interrupts any handler that is still running.
4. Storage closes, then the listener stops.

The Durable Object host does not get this signal. Streamsy code does not see a
shutdown there. Without `drainOnShutdown`, the routes keep today's behaviour.

| Entry                                                                   | Exports                                                                         |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `.`                                                                     | `Serve.stream`, `Serve.state`, `Serve.value`, their types; `PublicErrorSchema`  |
| `/contract`                                                             | Effect-free headers, public errors/decoder, canonical JSON, fingerprints        |
| `/bun`                                                                  | `listener`, `drainOnShutdown`, their options and defaults                       |
| `/cloudflare`                                                           | `StreamsyObject`, placement router/rules, alarm service                         |
| `/alchemy`                                                              | `objectHandlers`, placement router/rules, alarm service                         |
| `/action`                                                               | Unchanged action declarations                                                   |
| `/action/errors`, `/action/outbox`, `/action/runtime`, `/action/sqlite` | Unchanged action delivery and backing APIs, except unused OUTBOX_SCHEMA removed |

Ten physical entries form six groups. Action's existing Sink names remain.
State/stream recovery is `replay-from-start`; state consumers clear their local
collection before replay. Old sink headers and snapshot reset exports are gone.
Fingerprints cover route/address metadata and an optional `contract` revision;
change that revision when codec behavior changes without metadata changing.
Fingerprints are change detectors, not authorization or cryptographic identities.

The value contract header is `x-streamsy-value-contract` (`VALUE_CONTRACT_HEADER`
in `/contract`), and `Serve.value` options are `ValueOptions`. A projection's
served state is `Serve.value(declaration.state, path)`.

Guides: [Serve routes](https://streamsy.dev/docs/http/serve),
[Protocol routes](https://streamsy.dev/docs/http/routes),
[Cloudflare](https://streamsy.dev/docs/hosting/cloudflare),
[Alchemy](https://streamsy.dev/docs/hosting/cloudflare#alchemy).
