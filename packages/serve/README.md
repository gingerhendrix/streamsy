# `@streamsy/serve`

Every checked sink contract in the repository, the Effect server adapters that
serve them, and the action-sink delivery runtime.

Four sink families live here. Each one is an inert declaration compiled from a
route, a set of parameter codecs, and a written-out fingerprint; the runtime
that serves it is a separate module behind a separate subpath.

A **state sink** publishes a keyed collection as durable state a consumer reads
and resumes. `defineStateSink` is the package root, because it is the contract
the browser binding and the code generator are built on. Its declared fallback
is `snapshot-then-live`.

A **stream sink** publishes what happened to a relation. Its input is a change
stream — `streamSink(changes(issues))` — and its only ordering promise is the
one the engine can honour: Durable Stream arrival order. Nothing re-sorts by a
domain field, so a producer that needs a domain order must append in that
order. A consumer resumes by native offset, and the declared fallback is
`replay-from-start`.

A **document sink** publishes one derived value per route: a summary, a report,
a manifest. It has no resume position, because a consumer either holds the
current document or it does not. Instead it declares the relations it is
derived from, its own schema decode, and an explicit cache policy that lowers
to exactly one `cache-control` header.

An **action sink** is the one family that does not publish anything to read. It
delivers an external effect at least once, through a durable outbox, a
serialized retry budget, and a dead-letter terminus. It lives under `/action`.

## Subpaths

The Effect-free tier is the package root and the sibling subpaths beside it. No
module reachable from any of them imports `effect`, so a browser bundle that
declares or decodes a sink stays free of the runtime.

| Subpath          | Owns                                                                       |
| ---------------- | -------------------------------------------------------------------------- |
| `.`              | the state sink surface: protocol headers, `defineStateSink`, error union   |
| `./stream`       | the stream sink surface: protocol headers, `defineStreamSink`, error union |
| `./document`     | the document sink surface: protocol header, `defineDocumentSink`, errors   |
| `./route`        | `compileSinkRoute` and the parameter codec contract                        |
| `./route-params` | the compile-time check that a route and its codecs agree                   |
| `./fingerprint`  | canonical encoding, contract fingerprints, entity tags                     |

Each family is one module. Its wire protocol, its checked contract, and the
browser-safe error union a consumer decodes have one owner between them, in that
order within the file.

## Effect tier

| Subpath             | Owns                                                          |
| ------------------- | ------------------------------------------------------------- |
| `./server/state`    | `handleStateSink` and the server-side error union schema      |
| `./server/stream`   | `handleStreamSink`                                            |
| `./server/document` | `handleDocumentSink`                                          |
| `./action`          | `defineActionSink` and the checked action-sink types          |
| `./action/errors`   | the action sink's failures and dead-letter reasons            |
| `./action/outbox`   | the durable outbox contract and its in-memory backing         |
| `./action/runtime`  | the serialized delivery runtime and drain loop                |
| `./action/sqlite`   | the SQLite outbox backing and its migration                   |
| `./cloudflare`      | placement routing and the scoped Durable Object protocol host |
| `./alchemy`         | Effect object handlers and the Alchemy HttpEffect router      |

There is no barrel. A subpath points at the module that owns the symbols, so an
import names where a symbol lives.

## Shared rules

Route compilation is `compileSinkRoute` from `./route`, so every checked sink
speaks one route dialect rather than four.

No contract has an authorization concept. Access control belongs at the HTTP and
session boundary that wraps these handlers.

Entity tags are computed from a canonical encoding — object keys sorted, array
order preserved — so a document that differs only in property order keeps its
validator. Fingerprints and entity tags are change-detection identities, not
security digests.

`handleStateSink`, `handleStreamSink`, and `handleDocumentSink` own the public
protocol: route matching, version and contract negotiation, resume, the declared
decode, entity tags, and conditional requests. Reading a feed and building a
document are capabilities a host supplies.

Requires `effect@4.0.0-rc.112` on the `/server` and `/action` subpaths.

## Hosts

The Bun host owns one listener and one HTTP edge. `start()` builds
`HttpServer.serve(Http.app(options))` over `BunHttpServer`, reports the bound
address, and returns a `stop` Effect that closes the host scope. `layer()`
is the same composition as a Layer, for `Layer.launch`. The caller's reader and
writer Layer is provided at build time; `stop` drains in-flight requests and
disposes the scope. Bun has no automatic Layer rebuild policy. The drain is
unbounded by default, so a request that never finishes holds `stop` open; set
`gracefulShutdownTimeout` to bound it. The subpath needs
`@effect/platform-bun@4.0.0-rc.112`, which is an optional peer.

The Cloudflare entry keeps one scoped Layer per in-memory Durable Object and
shares it across `fetch` and `alarm`. `StreamsyObject.make({ options, layer })` takes HTTP options and a callback
from instance state and environment to the reader, writer, and storage Layer.
The factory provides the `ObjectOptions` service used by the exported `fetch`
effect; the exported `alarm` effect sweeps directly, without an HTTP request. A failed acquisition
returns `503 Storage unavailable` with `retry-after: 1`; the failed runtime is
discarded and the next request retries acquisition. See the complete
[hosting reference](../../docs/hosting.md) for placement, routing, forks,
expiry, cancellation, authorization, and evidence boundaries.

Local workerd, memory, and Bun SQLite each have an accepted 332-pass/6-skip
official profile. The workerd profile deliberately uses one `byKey` object for
same-object chain semantics. Forks require the source and child to share an object;
see [fork placement](../../docs/hosting.md#forks). Hosted
Cloudflare execution and release acceptance remain pending.

## Alchemy

`@streamsy/serve/alchemy` exports exactly `fetch`, `alarm`, `alarmLayer`,
`router`, `Placement`, and `ObjectOptions`, and has an optional peer on
`alchemy@2.0.0-beta.76`. The request effect reads the `ObjectOptions` service.
This sixth export lets Alchemy construction import the service under Bun
without loading the class entry’s `cloudflare:workers` dependency. The
Cloudflare entry continues to export the same service for class-form users.
In the runtime construction phase, supply that service beside storage and
`alarmLayer(state.raw.storage)` in one Layer. Cache the lazy build in the
object's scope, answer acquisition failures with 503 and retry on the next
call. The [typed construction fixture](test/alchemy/usage.ts) and its
[construction helper](test/alchemy/runtime.ts) show the complete composition.
Alchemy's alarm callback closes the typed error channel with `Effect.orDie`,
so a failed sweep reaches platform retry.

The router accepts `{ objects, pathPrefix, placement }`, strips the prefix for
placement, and forwards the original Effect HTTP request through
`objects.getByName(name).fetch(request)`. Its typed error is `HttpServerError`.
Authenticate before evaluating it. The class factory and the `ExportedHandler`
router remain the wrangler/Miniflare path; this subpath does not deploy resources.
