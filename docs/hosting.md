# Hosting reference

This reference describes the host code that is present in Streamsy 0.4.0 and
the evidence that is still missing. The Bun host and the Cloudflare Durable
Object host are implemented and have local tests. Cloudflare hosted execution
and release acceptance are still pending. celld is unconfirmed, outside the
supported-host set, and evidence-only.

The fixed release status is:

> Hosted execution and acceptance remain blocked by remote permission, the missing uploaded-compressed-byte/startup-CPU policy, and Gareth's budget/topology decision. The accepted Batch B local signal is 81,574 B gzip against the unchanged 27,160 B proposal. The 542.85 ms first-object p95 proposal remains unmeasured.

## Host ownership

`@streamsy/serve/bun` exposes `serve()`. It owns one listener and one HTTP
edge. `stop()` closes connections, waits for active handlers and reads, and
disposes the acquired scope. The caller supplies the reader/writer Layer;
there is no automatic Layer rebuild policy for Bun. A Layer acquisition error
occurs at the first lazy request that needs it and does not imply that the
process terminates.

`@streamsy/serve/cloudflare` exposes `router`, `Placement`, and
`StreamsyObject`. Each in-memory object owns one edge/Layer scope shared by
`fetch` and `alarm`. The object supplies its reader, writer, and storage Layer
from `layer()`. Durable Object SQLite persists beyond the in-memory lifetime;
the host claims no platform disposal hook. If acquisition fails, fetch returns
`503 Storage unavailable` with `retry-after: 1`, discards the failed edge, and
tries acquisition on the next call.

The storage runtime uses Reactivity push and its existing 1,000 ms repair tick.
Expiry is lazy on Bun access, with optional caller-driven expiry. In a Durable
Object, an alarm invokes a private in-process command; reconciliation follows
PUT, POST, DELETE, and alarm turns. Lazy access and later mutations repair a
missed or exhausted alarm. The minimum arm time is now +1 ms. Platform retries
are finite and owned by the platform, not by an application retry loop.

Accepted local alarm evidence on pinned workerd observed the first retry about
2.3–2.5 seconds later, with a scheduled timestamp delta of zero. This is an
observation of that local pin, not a platform cadence or a guaranteed retry
count. Hosted cadence, exhaustion, and `deleteAlarm` behavior remain
unmeasured.

## Defaults and cancellation

The Bun long-poll default is 30,000 ms. The
`@streamsy/storage/durable-object` `layerProtocol` default is 25,000 ms. Core
SSE connections are bounded at 60,000 ms on both hosts. The default request
body limit is 1 MiB and is configurable through the existing host options. The
local conformance Worker intentionally overrides the Durable Object long poll
to 1,500 ms; that is a test setting, not a production default.

Bun request aborts interrupt pending reads. The pinned workerd client-disconnect
propagation diagnostic remains red; the default local proof is bounded release
at the protocol deadlines. Hosted disconnect propagation is unmeasured.

## Routing, placement, and authorization

The router and object use the same literal `{ pathPrefix, placement }` pair.
The default placement is `byStream()`. `byKey()` is a pure function of the
path after its prefix is stripped. Keep this mapping stable for stored data.
Invalid placement returns the accepted route errors; there is no magic
placement header. A prefix is a routing boundary, not authentication.

Neither host implements authorization. Authenticate and authorize before
forwarding to the router, derive tenant and path mapping from the authenticated
identity, and enforce it there. A caller-chosen prefix or object id is not an
isolation boundary; possession of a namespace binding reaches its objects.

The internal `streamsy.internal/fork-source` authority grants no privilege. A
nonempty prefix keeps it off public routes; an empty-prefix object can still be
reached with that authority. Private expiry authority is an in-process Context
value, never a request marker or header.

## Forks and copy limits

Same-object chain forks are atomic in one database. Cross-object forks own a
bounded copied prefix, retain provenance for retry identity, and do not retain
the parent. The default `copyOnForkMaxBytes` is 8,388,608 encoded frame bytes,
including 45 bytes of per-message overhead. Snapshot source incarnation/count
checks and a destination atomic commit protect the copy; this is not a
distributed transaction. The source may change after a valid snapshot. The
accepted same-millisecond `createdAt` identity residual remains a known limit.

An over-budget copy returns `409 Fork copy exceeds copyOnForkMaxBytes`. For an
otherwise valid sufficiently long source, a cross-object JSON sub-offset above
10,000 returns `400 Stream-Fork-Sub-Offset exceeds source message count`; Bun
can return 201 for the corresponding case, and text/binary large-sub-offset
parity is accepted. A detected changed or vanished snapshot returns
`500 Internal server error`, after which the caller may retry following object
recreation. This does not add a 503/Retry-After promise, and a missing initial
source is not universally a 500. Same-object forks are not subject to the
cross-object copy cap.

The accepted estimate is about three times the encoded copy budget per request
on each side. It is not a strict process-memory maximum, and there is no global
concurrency or memory cap. The retained per-request memory, hosted
`idFromName` length, and same-millisecond identity limits are open observations,
not new scope.

## Local evidence and hosted boundary

The local official suite has three registrations: memory, Bun SQLite, and
workerd. Each accepted profile reports 332 passed and 6 skipped. The workerd
profile deliberately uses `byKey(() => "conformance")` so all suite streams
exercise one object and same-object chain semantics. Accepted cross-object copy
tests run separately in the real local workerd host tests. The default
`byStream()` profile retains nine chain-lifecycle divergences; those failures
are a topology distinction, not a changed assertion or a hosted result.

The Worker artifact is local-only and has one output module. The accepted C
artifact report records raw 534,562 B, minified 252,866 B, deterministic stdin
gzip 81,486 B, 116 input modules, and the worker SHA-256
`2b32a617c8129a4f805754c398e67da963935d5c9cbb58d3f7849ef760c5e898`. These
figures are labeled local and do not establish uploaded compressed bytes,
startup CPU, or a budget pass.

The isolated `hosted/` package typechecks the pinned Alchemy v2 stack and tests
a fake-only Effect workflow. Its executable is intentionally blocked: `--help`
prints the purpose and fixed status with exit 0, while evidence invocation
prints the status and “Live hosted adapters are not enabled in this local range”
with exit 2, even when fake credentials or permission values are supplied. No
live deploy, destroy, metadata query, remote conformance, hosted measurement,
or celld operation is reachable from the default commands. Later enablement
needs separately reviewed live adapters, cleanup, measurement policy, and
Gareth's budget/topology decision.

Metadata fields remain distinct. Reported script size, downloaded module bytes,
actual uploaded compressed bytes, and startup CPU are separate availability
records; a missing value is unavailable, never zero or a substitute metric.

Accepted implementation limits remain limits: the swallowed-fault logging seam,
same-millisecond incarnation hardening, the proposed fail-closed `503` policy,
migrations outside the yield override, per-request copy memory, hosted
`idFromName` length, platform alarm behavior, and the workerd abort diagnostic.
This documentation does not reopen those items or enlarge the local scope.

## Local ledger

The D-local gate starts at accepted C commit
`e09213c1f88e1f68f3e1c3f8a556baa442fb430b`. The reproducible command sequence
is recorded in the stream result and includes build, typecheck, hosted fake
workflow, unit and three-backend conformance suites, SQL boundary, bundle
measurement, lint and perimeter policy, format, package dry runs, and
`site:validate`. Site validation checks terms, compiled excerpts, the local
Cloudflare usage example, site build/OG output, and rendered internal links.

All claims in this page are either accepted C evidence, the local source
contract, or explicitly marked as an observation or deferred gate. No claim in
this reference authorizes or performs a remote operation.
