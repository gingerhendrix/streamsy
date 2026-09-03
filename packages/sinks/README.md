# `@streamsy/sinks`

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

| Subpath               | Owns                                                             |
| --------------------- | ---------------------------------------------------------------- |
| `.`                   | `defineStateSink` and the checked state-sink types               |
| `./state-errors`      | the state sink's browser-safe public error union and its decoder |
| `./state-protocol`    | the state sink's wire headers and protocol identifier            |
| `./stream`            | `defineStreamSink` and the checked stream-sink types             |
| `./stream-errors`     | the stream sink's public error union                             |
| `./stream-protocol`   | the stream sink's wire headers                                   |
| `./document`          | `defineDocumentSink` and the checked document-sink types         |
| `./document-errors`   | the document sink's public error union                           |
| `./document-protocol` | the document sink's wire header                                  |
| `./route`             | `compileSinkRoute` and the parameter codec contract              |
| `./route-params`      | the compile-time check that a route and its codecs agree         |
| `./fingerprint`       | canonical encoding, contract fingerprints, entity tags           |

The Effect tier:

| Subpath                       | Owns                                                           |
| ----------------------------- | -------------------------------------------------------------- |
| `./server/state`              | `handleStateSink`                                              |
| `./server/stream`             | `handleStreamSink`                                             |
| `./server/document`           | `handleDocumentSink`                                           |
| `./server/state-error-schema` | the server-side schema for the state sink's public error union |
| `./action`                    | `defineEffectSink` and the checked action-sink types           |
| `./action/errors`             | the action sink's failures and dead-letter reasons             |
| `./action/outbox`             | the durable outbox contract and its in-memory backing          |
| `./action/runtime`            | the serialized delivery runtime and drain loop                 |
| `./action/sqlite`             | the SQLite outbox backing and its migration                    |

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
