# issue-tracker-projections

A projection-heavy issue tracker built on the experimental Streamsy mesh. It
proves one real multi-step durable path:

```text
IssueEvents(issueId)
  -> IssueDetail(issueId)        recovered single-source State projection
  -> ProjectBoard(projectId)     deterministic dynamic fan-in State projection
  -> browser
```

`examples/issue-tracker-demo` remains the simple baseline. This example is the
projection demo and is deliberately separate.

Two architectural commitments run through the whole example:

- **Effect-first.** The server is a set of Effect descriptions with declared
  services and typed errors. `server/local.ts` and `server/worker.ts` are the
  only executable edges.
- **Alchemy v2.** The deployment program is an `Alchemy.Stack` Effect, not a v1
  script of top-level `await`s.

Both are covered by tests: `test/architecture.test.ts` and
`test/alchemy-stack.test.ts`.

## Server architecture

Every application operation is an `Effect` that declares what it needs and how
it can fail. Nothing takes an options record, resolves a client, converts a
Promise, or throws to signal an expected failure.

```text
server/errors.ts       typed failures (Schema.TaggedErrorClass)
server/config.ts       AppConfig service, read through Effect Config
server/streams.ts      Streams service — the only protocol-client boundary
server/lanes.ts        ProjectionLanes service — Cache-backed producer lanes
server/commands.ts     CommandProducers service + append classification
server/wake.ts         Wake service — disabled locally, queue-backed on Workers
server/runtime.ts      the application Layer, assembled once
server/application.ts  command and query workflows
server/router.ts       the trust boundary: decode, call, translate by _tag
```

- **Services and layers.** `server/runtime.ts` is the single place the
  dependency graph is written down. A host picks storage, a configuration
  source, and a wake lane; a test picks whatever it wants to observe.
- **Typed errors.** `InvalidRequest`, `MalformedBody`, `UnknownProject`,
  `UnknownIssue`, `AppendRejected`, `StreamUnavailable` — plus the mesh's own
  `StateRestorePoison`. The router maps them by `_tag`. A defect reaching the
  edge is a bug, and is reported as an opaque `500`.
- **Schema at the boundary.** Request bodies are decoded by the schemas in
  `shared/requests.ts` before any workflow sees them, so an unknown status, an
  unknown team member, or a wrong-typed field is a `400` and never durable
  state. Downstream code has nothing left to validate.
- **Scoped runtime ownership.** Each host owns exactly one `ManagedRuntime` for
  its lifetime; read sessions are released by `Effect.scoped` at their own
  boundary.
- **Adapter boundaries only.** The three async platform APIs this example
  depends on — stream creation, `crypto.subtle.digest`, and `deriveProducerLane`
  — are wrapped once inside their service, not at call sites.

`src/` stays ordinary React and ordinary TypeScript. The Effect runtime is not
in the browser bundle.

## The workspace UI

`src/` is a React workspace served as a static bundle from `dist/assets`:

- a project rail with durable counts and inline project creation;
- a three-column board with drag-and-drop **and** a per-card status select, so
  every movement is completable from the keyboard;
- an issue drawer (a full-screen sheet on mobile) for title, status, priority,
  assignee, and comments;
- optimistic patches that are display overlays only — they expire against the
  durable row, or after a bounded hold, and never become accepted state;
- a projection inspector that labels the three durable identities of the latest
  command, reports `Proven` / `Not yet` / `Incomparable` from server lineage,
  and lists the classified outcome of every projection pass;
- visible failures with a `Retry sync` action that replays the _newest failed_
  command by its own `commandId`, so a retry reconciles instead of duplicating
  and never replays an older command for the same issue.

The browser reads durable **State streams** directly over the Durable Streams
HTTP endpoint (catch-up read, then long-poll live reads). Nothing on screen is
reconstructed from command responses, so a reload — or a second window — is
rebuilt from the board State stream alone.

The URL (`?workspace=…&project=…&issue=…`) is the shareable source of truth.

### `Synced` means proven

`Synced` has exactly one meaning here: the accepted source acknowledgement is
durably covered by the project board. A card, chip, or header only says it when
the server reported `coverage.status === "proven"` **and** every projection pass
it ran came back `caught-up`.

Everything else is visible as something weaker:

| Server result                                                       | UI        |
| ------------------------------------------------------------------- | --------- |
| proven coverage, all passes caught up                               | `Synced`  |
| `not-yet` / `incomparable` coverage, or a `deferred` pass           | `Pending` |
| a faulted pass — output conflict, poison, unknown member, oversized | `Failed`  |
| the request itself failed                                           | `Failed`  |

A `Pending` mutation runs a **bounded** convergence loop in the browser: repair,
then a read-only `GET .../issues/{id}/coverage?position=…` lineage probe, up to
six attempts. Only a probe that returns proven coverage promotes it to `Synced`.
If the bound is exhausted the mutation stays `Pending` with a note saying so —
it is never upgraded on elapsed time, and it is never downgraded to a failure it
did not have.

Pending overlays are never retired on a timer, because an unproven mutation has
nothing durable to expire against.

Append `&defer=1` to the workspace URL to make the browser send
`?projections=deferred` with every command. The immediate passes are skipped, so
the `Pending` state is reachable by hand — and `smoke:ui` uses it to drive the
whole `Pending → repair → probe → Synced` path in a real browser.

## Streams

```text
workspaces/{workspaceId}/projects
workspaces/{workspaceId}/issues/{issueId}/events
workspaces/{workspaceId}/issues/{issueId}/detail
workspaces/{workspaceId}/projects/{projectId}/membership
workspaces/{workspaceId}/projects/{projectId}/board
```

Stream identity and stream id are kept equal so durable lineage is readable by
inspection. Issue truth is split per issue; the board fans in over the project's
active issue-detail streams.

## Semantics worth knowing

- **Exact acknowledgement.** Every command carries a `commandId`, which becomes a
  producer lane on the issue events stream. A retried command returns
  `duplicate` and the API reports the _original_ offset with `reconciled: true`.
  Payload equality is never claimed. Project creation follows the same rule: a
  repeated `projectId` returns the **durable** project row with `200`, not the
  new request payload, and a rejected append is a `409` rather than a `201`.
- **Chained coverage.** A mutation reports `proven` only when durable lineage at
  both hops covers the accepted source position. A wake receipt or elapsed delay
  can never produce `proven`.
- **Classified projection passes.** Every response carries a `projections` array:
  each pass reports its exact kernel status and an outcome of `caught-up`,
  `deferred`, or `faulted`. A pass status is never discarded, and a typed mesh
  error — `StateRestorePoison` included — becomes a `faulted` pass rather than a
  lost result.
- **Schema-backed durable values.** Restored and served `IssueDetail`, `BoardRow`,
  and `Project` values are decoded through Effect Schemas. A row carrying the
  right collection tag but a malformed application value is rejected: in a
  projection restore the throw becomes the kernel's typed `StateRestorePoison`,
  and the board endpoint answers `500 state-restore-poison` rather than serving
  the value.
- **Deferred projections.** `POST …/commands?projections=deferred` (and the same
  option on issue creation) skips the immediate passes. Durability and the
  acknowledgement are unchanged; only the latency optimisation is dropped, so a
  smoke can exercise the queue and repair path a lost immediate pass depends on.
- **Wake is latency, repair is the guarantee.** The mutation request runs both
  projections immediately for low latency. `POST .../projects/{id}/repair` and
  the Cloudflare queue consumer run the same bounded work, so a lost pass
  converges.
- **Recovery cost.** Both kernels restore application state by scanning complete
  target history. That is O(history) and intentional; snapshots are deferred.
- **Issue keys.** The display key is derived from the count of durable join facts
  in the project. Concurrent creation can repeat a display key; key uniqueness is
  not a correctness law here.

## Decision: a custom durable-stream reader, not StreamDB/TanStack DB

The browser tail is roughly 100 lines in `src/lib/stream.ts` plus a pure fold in
`src/lib/state.ts`. It is **not** routed through StreamDB or TanStack DB, and
that is a deliberate, recorded scope choice rather than an oversight.

Why:

- the demo's claim is about durable **State-stream reconstruction**, and a small
  explicit reader makes that claim inspectable — the catch-up read, the resume
  offset, and the fold are all visible in one file;
- it keeps the browser bundle free of a client-database dependency, so what the
  reload actually rebuilds from is unambiguous;
- adding the integration would mean a new pinned dependency and a migration of
  the whole read path, which is a change of a different size to the correctness
  fixes this batch carries.

What it costs: the intended StreamDB/TanStack DB client integration is
**unproved** by this example. Local tests cover the fold, the resume behaviour,
and durable reconstruction after a reload, but nothing here exercises that
library path. Routing the UI through StreamDB remains open work and should be
its own change.

## Local development

```bash
bun install
bun run build
bun run --cwd examples/issue-tracker-projections dev      # http://localhost:8787
curl -X POST http://localhost:8787/api/workspaces/main/seed
open 'http://localhost:8787/?workspace=main&project=launch'
```

The root build produces the workspace package entry points consumed by this example. Re-run it
after changing a workspace package under `packages/`; the example's own `build` script separately
produces its browser assets and Worker bundle.

`dev` builds the browser bundle into `dist/assets` first; the local host serves
that directory, so re-run `build` after changing anything under `src/`. Seeding
is idempotent — the "Seed demo workspace" button in an empty workspace runs the
same command path.

The local host also serves the Durable Streams HTTP routes under `/streams/`, so
the board State stream is readable directly at
`/streams/workspaces/main/projects/launch/board`.

## Checks

```bash
bun run --cwd examples/issue-tracker-projections typecheck
bun run --cwd examples/issue-tracker-projections test       # vitest + bun sqlite
bun run --cwd examples/issue-tracker-projections build
bun run --cwd examples/issue-tracker-projections smoke:http
bun run --cwd examples/issue-tracker-projections seed:check
bun run --cwd examples/issue-tracker-projections audit:state
bun run --cwd examples/issue-tracker-projections deploy:check
```

`test/architecture.test.ts` is the architecture's own regression suite. It runs
application operations on runtimes built from ad-hoc layers, asserts that an
unknown project surfaces as a typed `UnknownProject` in the error channel,
proves malformed bodies are rejected before any durable write, swaps the `Wake`
layer to observe that only an _unproven_ command wakes anything, and drives
`AppConfig` from a `ConfigProvider`. None of those pass against a Promise-first
implementation with Effect wrappers.

`smoke:http` also runs the deferred-projection path end to end: a command with
`?projections=deferred`, a read-only probe that must _not_ report proven, a
repair pass, and a second probe that must report `coverage.status === "proven"`.

`smoke:ui` drives a production build in a real browser: keyboard issue creation,
drawer edits, comments, the accessible status control, inspector coverage and
focus handling, a reload rebuilt from durable State, second-window convergence,
the deferred `Pending → proven → Synced` path, an unclipped 390px header, and the
mobile sheet. It fails on any console error or failed application request.
Playwright
is not a repository dependency, so the script skips when it is unavailable and
`scripts/ui-smoke.ts` is excluded from `typecheck`:

```bash
bun add -g playwright-core && bunx playwright install chromium
bun run --cwd examples/issue-tracker-projections build
PLAYWRIGHT_EXECUTABLE=<chrome binary> bun run --cwd examples/issue-tracker-projections smoke:ui
```

## Deployment: Alchemy v2

`alchemy.run.ts` is an Alchemy **v2** program. A v2 stack is an Effect, not a
script:

```ts
export default Alchemy.Stack(
  "streamsy-issue-tracker",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const wakes = yield* ProjectionWakes;
    const api = yield* Api;
    yield* Cloudflare.Queues.Consumer("WakeConsumer", {
      queueId: wakes.queueId,
      scriptName: api.workerName,
    });
    return { url: api.url, worker: api.workerName, queue: wakes.queueName };
  }),
);
```

What that buys, concretely:

- **Importing the program applies nothing**, because it is a description. That is
  why `test/alchemy-stack.test.ts` can import the real program and assert its
  shape rather than a copy of it.
- **`alchemy plan` is a real read-only operation.** v1 had none, so the old
  `deploy:check` could only typecheck and build. `deploy:check` now evaluates
  the stack and prints the resources a deploy would create.
- **The Worker's `env` type is derived, not restated.**
  `server/worker.ts` types its bindings as `Cloudflare.InferEnv<typeof Api>`, so
  a renamed or removed binding is a `typecheck` failure.
- **The Durable Object is declared inline** as
  `Cloudflare.DurableObject<DurableObjectStreamStorage>("StreamDO", { className: "StreamStorage" })`.
  SQLite storage is the v2 default for a class the Worker hosts itself, so v1's
  `sqlite: true` flag is gone.

| Script             | Contract                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| `build`            | produce browser assets and the Worker bundle                                                     |
| `deploy:check`     | typecheck, build, stack-shape tests, `alchemy plan`, state audit — changes nothing               |
| `plan:demo`        | `alchemy plan` on its own                                                                        |
| `audit:state`      | fail if any applied Alchemy state carries a runtime identifier                                   |
| `deploy:demo`      | deploy an isolated named stage (`STAGE=...`) and print the URL                                   |
| `smoke:deployment` | health, a deferred mutation, repair to proven coverage, and a durable re-read against `DEMO_URL` |
| `destroy:demo`     | destroy only that stage                                                                          |

```bash
STAGE=demo bun run --cwd examples/issue-tracker-projections plan:demo
STAGE=demo bun run --cwd examples/issue-tracker-projections deploy:demo
DEMO_URL=https://... bun run --cwd examples/issue-tracker-projections smoke:deployment
STAGE=demo bun run --cwd examples/issue-tracker-projections destroy:demo
```

Alchemy owns one Worker, one SQLite-backed `StreamStorage` Durable Object
namespace, one wake queue with its consumer, and the static assets. It owns no
workspace, project, issue, cursor, membership, or lineage value; all of those are
Streamsy runtime state inside the Durable Objects.

### Dependency note

Alchemy v2 is itself built on Effect v4 beta. Some of its transitive
`@distilled.cloud/*` packages declare a looser `effect` peer range than they can
actually run against, so the repository root pins `effect` (and
`@effect/platform-node`) in `overrides`. Without that pin the CLI loads two
copies of Effect and fails at start-up. `packages/conformance-tests` still uses
the catalog's Alchemy 0.x; only this example is on v2.

### Evidence limits, stated precisely

- `deploy:demo` and `smoke:deployment` have **not** been run against a live
  account: no Cloudflare credentials were available. `deploy:check` passes, which
  proves the v2 program typechecks against the installed declarations, that the
  bundle builds, and that `alchemy plan` resolves the stack to exactly the
  Worker, its four bindings, the queue, and the consumer — not that the topology
  applies to Cloudflare.
- `audit:state` reports **skipped**, not passed, in this environment.
  `.alchemy/state` only exists after a real deploy, so there is no applied state
  to scan. The claim that deployment state holds no runtime identity currently
  rests on the topology in `alchemy.run.ts` — which `test/alchemy-stack.test.ts`
  checks with the same patterns — and the audit verifies it against applied
  state the moment a stage exists.
- `smoke:deployment`'s re-entry check is a **second HTTP request observing the
  same durable rows**. It does not force a new Worker isolate or a Durable Object
  eviction, and the script says so in its output rather than claiming cold
  re-entry evidence it did not gather.
- The queue consumer path is exercised locally through the equivalent bounded
  repair. Live Cloudflare queue delivery remains unverified.
