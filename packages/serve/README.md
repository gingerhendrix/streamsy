# @streamsy/serve

Effect HttpRouter routes for streams, Durable State and JSON values, plus Bun,
Node, Deno and Cloudflare host glue and durable action delivery. Requires Effect
`4.0.5`.

```ts
import { Layer, Schema } from "effect";
import { BunRuntime } from "@effect/platform-bun";
import { Http, Streamsy, MemoryStorage } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";

class Events extends Streamsy.json<Events>()("Readme/Events", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}
const streams = Streamsy.group(Events);
const App = Layer.mergeAll(
  Http.routes(streams, { prefix: "/streams", access: "full" }),
  Serve.mount(Events, "/feed/:seat"),
);
const Server = Serve.layer(App).pipe(
  Layer.provide(MemoryStorage.layer(streams)),
  Layer.provide(listener({ port: 3000 })),
);
BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
```

`Serve.stream(address)` and `Serve.state(address)` return response Effects.
State serves changes from `-1`, with no snapshot. `Serve.value(schema, effect)`
serves canonical JSON with a deterministic ETag. Map `Projection.read` to serve
custom values; an unsaved projection returns its declared initial value.
An absent stream answers 404. Reads create no stream, checkpoint or state row.

`Serve.params(Stream)` decodes path parameters. `Serve.mount(Stream, path)`
mounts a declared stream or state. `Serve.route(path, handler)` owns all methods:
GET and HEAD read; others return 405 with `allow: GET, HEAD`, unless CORS
middleware answers a preflight. Handlers also compose inside app routes and
`HttpApiBuilder.handleRaw`. Their respondable errors remain catchable.
Auth can provide a service from which the handler builds an address.
Protect the protocol routes too when their underlying data is private.

Bun's listener defaults to `idleTimeout: 0`. Route handlers are interruptible
on the pinned Effect/Bun router; client abort releases a parked long poll.
The Node listener keeps the `node:http` defaults, which do not cut a parked
long poll or an SSE body, and takes `server` options for `createServer`. The
Deno listener needs no timeout setting. On Deno, run `nodeSqliteCompat` before
an `@effect/sql-sqlite-node` client opens: Deno's `DatabaseSync` lacks the
`enableLoadExtension` method that the driver calls.
The caller owns the Layer scope. Tests or custom hosts can use Effect's
`HttpRouter.toWebHandler(App)` and must call its `dispose` at shutdown.

### Long-running hosts and shutdown

`Serve.layer(App, options)` is `HttpRouter.serve(App, options)` with the drain
for live readers built in. It takes the options of `HttpRouter.serve`, plus
`drainTimeout` (default 2 seconds). Without a drain, closing the serve layer
interrupts every handler: a parked long poll gets an empty `503` and an open
SSE body is reset with no final event. With `Serve.layer`, the shutdown order
is:

1. The drain signals every live read. A parked long poll answers `204` with
   `Stream-Next-Offset`, `Stream-Up-To-Date` and a fresh `Stream-Cursor`, the
   same as a long-poll timeout. An open SSE body ends after its last whole
   batch, which ends with a control event.
2. The drain waits until no live read is running, or until `drainTimeout`
   passes. A live request that arrives in this window gets the same reply at
   once.
3. The serve layer closes and interrupts any handler that is still running.
4. Storage closes, then the listener stops.

`Serve.teardown` is the exit policy for `runMain`. An interrupt-only exit, such
as a stop by `SIGINT` or `SIGTERM`, exits 0 in place of Effect's 130. Any other
exit keeps `Runtime.defaultTeardown`, so a startup failure stays non-zero.
Both live in the root entry and depend on no runtime. A host subpath gives only
the listener.

The Durable Object host does not get a stop signal and uses neither.

| Entry                                                                | Exports                                                                                                                                                            |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `.`                                                                  | `Serve.stream`, `Serve.state`, `Serve.value`, `Serve.params`, `Serve.route`, `Serve.mount`, `Serve.layer`, `Serve.teardown`; errors and types; `PublicErrorSchema` |
| `/contract`                                                          | Effect-free headers, public errors/decoder, canonical JSON, fingerprints                                                                                           |
| `/bun`                                                               | `listener`, its options and defaults                                                                                                                               |
| `/node`                                                              | `listener`, its options and defaults                                                                                                                               |
| `/deno`                                                              | `listener`, its options and defaults; `nodeSqliteCompat`                                                                                                           |
| `/cloudflare`                                                        | `StreamsyObject`, placement router/rules, alarm service, `ObjectLink`, `ObjectStreams`                                                                             |
| `/alchemy`                                                           | `objectHandlers`, placement router/rules, alarm service, `ObjectLink`, `objectFetch`                                                                               |
| `/action`                                                            | Unchanged action declarations                                                                                                                                      |
| `/action/errors`, `/action/outbox`, `/action/runtime`, `/action/sql` | Action delivery and backing APIs; `createSqlOutboxBacking` runs on SQLite, Postgres and MySQL                                                                      |

Twelve physical entries form six groups. Action's existing Sink names remain.
State/stream recovery is `replay-from-start`; state consumers clear their local
collection before replay. Old sink headers and snapshot reset exports are gone.
Fingerprints cover route/address metadata and an optional `contract` revision;
change that revision when codec behavior changes without metadata changing.
Fingerprints are change detectors, not authorization or cryptographic identities.

The value contract header is `x-streamsy-value-contract` (`VALUE_CONTRACT_HEADER`
in `/contract`), and `Serve.value` options are `ValueOptions`. A projection's
served state comes from `Projection.read(declaration.at(params))`.

Guides: [Serve routes](https://streamsy.dev/docs/http/serve),
[Protocol routes](https://streamsy.dev/docs/http/routes),
[Cloudflare](https://streamsy.dev/docs/hosting/cloudflare),
[Host contract](https://streamsy.dev/docs/hosting/host-contract),
[Alchemy](https://streamsy.dev/docs/hosting/cloudflare#alchemy).

## Object links and projection wakeups

`ObjectLink.make({ group, prefix, placement, access })` from `/cloudflare` or
`/alchemy` declares an internal protocol mount and its client together. Add
`link.routes` to the owner app and the client to the caller's `links`.
The prefix must start with `/__`. Both public placement routers return 404
for paths whose first segment starts with `__`, without calling an object.
The owner still needs the explicit internal route.

Both hosts use one `ObjectLink` and one `LinkClient`. A native Cloudflare
object takes `links: (env) => [link.client(env.OWNERS)]`. Alchemy
`objectHandlers` takes `links: [link.client(objectFetch(owners))]`. A linked
declaration must not also be provided by local storage; the host rejects that
clash during acquisition.

`ObjectStreams.layer(group, { objects, placement, prefix })` builds the same
remote stream services for an existing protocol mount. `objects` is a native
namespace or an `ObjectFetch`: `(name, request) => Effect<Response,
HttpClientError>`. The client uses `HttpClient.make`, retaining caller context,
tracing and interruption. Alchemy's `objectFetch` adapts its stub Effects.

Access is mount policy for every stream. `Streamsy.wake` is a State preset
with the normal writer service. Use `read` for public projection outputs,
`full` when an internal producer creates its output, or `append` when the
owner creates it first. `Serve.mount` accepts GET and HEAD.

Object hosts acquire `ProjectionWakeup.attach(projection)` from
`@streamsy/projection` in their scope through `wakeups: [...]`. Local
`Storage.committed` events populate its in-memory pending set, and each commit
writes a durable pending mark in the same transaction. Alarms drain bounded
passes. After an eviction, acquisition reads the marks and arms the alarm.
Retry options take an Effect `Schedule`; finite schedules stop on exhaustion.
`Wakeups.status` reports each wakeup's deferred bindings.
See the [projection host reference](https://streamsy.dev/docs/hosting/cloudflare#projections).
