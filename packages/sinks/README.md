# `@streamsy/sinks`

Two checked sink contracts that publish something other than a keyed
collection, and the Effect server adapters that serve them.

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

Both are inert declarations. `defineStreamSink` and `defineDocumentSink` compile
the route, derive what they can from the declaration rather than restating it —
a stream sink's subject key is the key its change stream declares — and hash a
written-out fingerprint over the route, parameters, published metadata,
protocol, and declared errors. `handleStreamSink` and `handleDocumentSink` in
`@streamsy/sinks/effect` own the public protocol: route matching, version and
contract negotiation, resume, the declared decode, entity tags, and conditional
requests. Reading the feed and building the document are capabilities a host
supplies.

Neither contract has an authorization concept. Access control belongs at the
HTTP and session boundary that wraps these handlers.

Entity tags are computed from a canonical encoding — object keys sorted, array
order preserved — so a document that differs only in property order keeps its
validator. Fingerprints and entity tags are change-detection identities, not
security digests.

Route compilation is `compileSinkRoute` from `@streamsy/state-sink`, so every
checked sink in the repository speaks one route dialect rather than three.
