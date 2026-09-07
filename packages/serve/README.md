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

The Effect tier:

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

The Cloudflare host keeps one scoped Layer for an object and returns `503` with
`retry-after: 1` when Layer acquisition fails; it retries acquisition on the next
call. `@streamsy/storage/durable-object`'s `layerProtocol` defaults long-poll reads
to 25 seconds (Bun remains 30 seconds), and core bounds SSE connections at 60 seconds
on both hosts. Alarm retries are finite and platform-owned; lazy expiry on reads and
the next mutating request are the recovery after an exhausted or missed alarm. A
Worker with the namespace binding can address any object, because this host adds no
authorization. `StreamsyObject.options()` accepts `ObjectOptions`: its `placement` must
be the same `{ pathPrefix, placement }` configuration used by `router`, `namespace` enables
cross-object copy-on-fork, and `copyOnForkMaxBytes` is a positive safe-integer bound on
encoded frame bytes (8 MiB by default). Same-object forks remain atomic chains; cross-object
forks copy the selected prefix after one bounded, incarnation-checked snapshot, answer `409
Fork copy exceeds copyOnForkMaxBytes` when the bound is exceeded, and commit independently of
later source changes, expiry, recreation, or deletion. Cloudflare text and binary sub-offset
tails are capped at 10,000 messages for the internal snapshot; JSON sub-offsets above 10,000
are unsupported on that host. The `streamsy.internal/fork-source` frames representation is
kept off public routes when the router uses a non-empty `pathPrefix` (the recommended
configuration). With an empty prefix, a caller can select that representation by authority,
but it grants no privilege and is not a public protocol. A Worker holding the namespace
binding can address it directly. It exposes content already readable through the public
protocol plus message boundaries and timestamps; it is not an authorization credential. On pinned local workerd
1.20260730.1, client disconnects do not interrupt object reads; the protocol bounds
are the local release fallback. The propagation proof is available only with
`STREAMSY_WORKERD_CANCELLATION=1`, and is expected to fail on that pin.
