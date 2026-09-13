# Hosting reference

This reference describes the host code that is present in Streamsy 0.4.0 and
the evidence that is still missing. The Bun host and the Cloudflare Durable
Object host are implemented and have local tests. Cloudflare hosted execution
and release acceptance are still pending. celld is unconfirmed, outside the
supported-host set, and evidence-only.

The current release status is:

> Alchemy deployment for Step 3 is authorized. Hosted execution remains disabled in this local package pending independently reviewed live adapters and a reconciled run plan, including destroy/cleanup and required query scope. Hosted acceptance still requires hosted evidence, the uploaded-compressed-byte/startup-CPU policy, and Gareth's budget/topology decision. The accepted Batch B local signal is 81,574 B gzip against the unchanged 27,160 B proposal. The 542.85 ms first-object p95 proposal remains unmeasured.

## Host ownership

`@streamsy/serve/bun` exposes `layer()`, `listener()`, and `start()`. Together
they own one listener and one HTTP edge. `layer()` is the whole composition for
`Layer.launch`; `listener()` is the Bun listener alone; `start()` returns a
running host whose `stop` Effect closes connections, waits for active handlers
and reads, and disposes the acquired scope. The caller supplies the
reader/writer Layer; there is no automatic Layer rebuild policy for Bun. A Layer
acquisition error occurs at the first lazy request that needs it and does not
imply that the process terminates.

`@streamsy/serve/cloudflare` exposes `router`, `Placement`, and
`StreamsyObject`. Each in-memory object owns one runtime/Layer scope shared by
`fetch` and `alarm`. The object supplies its reader, writer, and storage Layer
through `StreamsyObject.make({ options, layer })`; the layer callback receives
the instance state and environment, and options become the `ObjectOptions`
service. Durable Object SQLite persists beyond the in-memory lifetime;
the host claims no platform disposal hook. If acquisition fails, fetch returns
`503 Storage unavailable` with `retry-after: 1`, discards the failed runtime, and
tries acquisition on the next call.

The storage runtime uses Reactivity push and its existing 1,000 ms repair tick.
Expiry is lazy on Bun access, with optional caller-driven expiry. In a Durable
Object, the alarm effect runs the sweep directly; reconciliation follows
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

The router takes `{ pathPrefix, placement, namespace }`; the object takes
`{ pathPrefix }` through its HTTP options.
The default placement is `byStream()`. `byKey()` is a pure function of the
path after its prefix is stripped. `byRoute()` matches `StreamRoute` values and
lets their decoded parameters select the owner. Keep this mapping stable for
stored data. An empty or non-string placement key returns
`400 Invalid placement key`; a placement callback that throws returns
`500 Internal server error`. There is no magic placement header. A prefix is a
routing boundary, not authentication.

Neither host implements authorization. Authenticate and authorize before
forwarding to the router, derive tenant and path mapping from the authenticated
identity, and enforce it there. A caller-chosen prefix or object id is not an
isolation boundary; possession of a namespace binding reaches its objects.

The exported `alarm` effect is not a reachable HTTP route. Request markers and
headers cannot select the expiry sweep.

## Forks

Forks require the source and child to share a Durable Object. The default
`Placement.byStream()` places each stream in a separate object, so it does not
support forks from another stream: the request returns
`404 Source stream not found: <source>`. Use `Placement.byKey(family)` with a
stable family key to co-locate each fork family. Cross-family forks also return
404 because the source is absent from the child's local storage.

Same-object forks chain atomically in one database and follow the core fork
rules, including source retention and cascade collection. The object uses the
ordinary protocol writer; it does not fetch or copy a remote source. The core
storage capability for copy forks remains available to other storage Layers.

## Local evidence and hosted boundary

The local official suite has three registrations: memory, Bun SQLite, and
workerd. Each accepted profile reports 332 passed and 6 skipped. The workerd
profile deliberately uses `byKey(() => "conformance")` so all suite streams
exercise one object and same-object chain semantics. Under the default
`byStream()` placement, the suite's forks cannot find their sources locally and
answer 404. This is a placement choice; the single-object profile is the fork
conformance profile, and local results do not establish hosted behavior.

The Worker artifact is local-only and has one output module. The clean S9
measurement at `cd0f915` records raw 554,771 B, minified 260,584 B,
deterministic stdin gzip 84,270 B, 127 input modules, and the worker SHA-256
`4a993b429846169a74d85b9e70f9cad9157d984bb01fcf0dac178ed5a2105678`. These
figures are labeled local and do not establish uploaded compressed bytes,
startup CPU, or a budget pass.

The isolated `hosted/` package typechecks two pinned Alchemy v2 stack entries:
the unchanged prebuilt conformance artifact with `bundle: false`, and a separate
source-form Effect Worker and Durable Object using `@streamsy/serve/alchemy`.
Neither stack is executed by the check, and source-form runtime readiness is not
established. The package also tests a fake-only Effect workflow. Its executable
remains disabled for evidence: `--help` prints the purpose and current status
with exit 0, while evidence
invocation prints the status and “Live hosted adapters are not enabled in this
local range” with exit 2, even when fake credentials or permission values are
supplied. No live deploy, destroy, metadata query, remote conformance, hosted
measurement, or celld operation is reachable from the default commands. Later
enablement needs separately reviewed live adapters, cleanup, measurement policy,
and Gareth's budget/topology decision.

Metadata fields remain distinct. Reported script size, downloaded module bytes,
actual uploaded compressed bytes, and startup CPU are separate availability
records; a missing value is unavailable, never zero or a substitute metric.

Accepted implementation limits remain limits: the swallowed-fault logging seam,
the proposed fail-closed `503` policy, migrations outside the yield override,
hosted `idFromName` length, platform alarm behavior, and the workerd abort diagnostic.
This documentation does not reopen those items or enlarge the local scope.

## Local ledger

The D-local gate starts at accepted C commit
`e09213c1f88e1f68f3e1c3f8a556baa442fb430b`. The complete reproducible command
sequence, required scratch environment, observed correction SHA and result are
recorded in the repository's [D-local verification ledger](d-local-verification.md)
and the stream's correction result. It includes build, typecheck, hosted fake
workflow, unit and three-backend conformance suites, ownership, SQL boundary,
bundle measurement, lint and perimeter policy, format, package dry runs, and
`site:validate`. Site validation checks terms, compiled excerpts, citation
drift tests, the local Cloudflare usage smoke check, site build/OG output, and
rendered internal links. The accepted hosted fake workflow is 37 passed/0
failed, with workerd ownership 8 passed/0 failed; these are local checks, not
hosted evidence.

All claims in this page are either accepted C evidence, the local source
contract, or explicitly marked as an observation or deferred gate. No claim in
this reference authorizes or performs a remote operation.
