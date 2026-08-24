# issue-tracker

The Streamsy issue tracker. This is slice 1 of the incrementally maintained
view DSL: one declaration carried end to end, from an HTTP command to a live
React board.

```text
HTTP issue command
-> canonical issue event append (producer lane = workspaceId + commandId, expected-offset CAS)
-> real workspace Durable Stream
-> typed source decode
-> reduceByKey issue lifecycle
-> SQLite issue rows, reducer state, command receipts, source checkpoint
-> one Durable State stateSink
-> caller-owned DurableStream + TanStack DB
-> React board
```

`examples/issue-tracker-demo` remains the simple baseline.
`examples/issue-tracker-projections` is the direct predecessor: this example
borrows its Effect-first server architecture and its durable proof obligations,
and presents the new DSL and sink contracts directly rather than preserving the
older hand-built projection API through an adapter.

## The declaration

`domain/declaration.ts` is the whole user-code surface:

```ts
export const issueEvents = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  partitionBy: x.row.workspaceId,
  mode: { kind: "facts", key: x.row.eventId, order: x.row.sequence },
});

export const issues = view(
  "issue-tracker.issues",
  { schema: IssueRow, key: out.row.issueId },
  from(issueEvents).reduceByKey({
    key: x.row.issueId,
    order: x.row.sequence,
    reducer: issueLifecycle,
  }),
);

export const boardIssues = stateSink("issue-tracker.board-issues", {
  from: issues,
  key: out.row.issueId,
  route: "/state/workspaces/:workspaceId/issues",
  params: ["workspaceId"],
  protocol: { transport: "durable-state", resume: true, fallback: "snapshot-then-live" },
  auth: scope("issue-tracker:workspace"),
});
```

Everything it builds is frozen, inert data. `@streamsy/views` lowers it to a
serializable `RelationPlan`, hashes that plan canonically, and
`views/engine.ts` is a pure interpreter of the plan. `GET /health` reports the
plan hash, so two hosts can be compared by inspection.

`@streamsy/views-ir` supplies the shared JSON-only contract promoted from the
`contracts-spike-minimal` stream (commit `11742f6`), narrowed to the vocabulary
this slice executes. Nodes the slice does not run — filter, project, key, left
join, grouped aggregate, top-N — are deliberately absent rather than declared
and unimplemented.

This branch also carries the smallest local source-mode compatibility contract
needed while A1 owns the public `@streamsy/views-ir` and `@streamsy/views`
packages. It encodes `facts` and `state` modes in plan version 2. Integration
should replace this local shape with A1's contract rather than publishing a
second IR.

## State sources

Projects, users, labels, and workspace metadata use four independent Durable
State streams:

```text
state/workspaces/{workspaceId}/projects
state/workspaces/{workspaceId}/users
state/workspaces/{workspaceId}/labels
state/workspaces/{workspaceId}/metadata
```

Each source decodes the State envelope, checks its collection, key, workspace,
and typed row, then commits current rows with that source's native checkpoint in
one application-store transaction. A bad immutable boundary is fail-stop: rows
and checkpoint remain unchanged. `delete` is decoded and returned as the typed
`UnsupportedStateOperation`; A3 intentionally implements upserts only.

Catalog rows are available at
`GET|POST /api/workspaces/:workspaceId/catalog/:collection`, where collection is
`projects`, `users`, `labels`, or `metadata`.

## Command reconciliation

Commands hash normalized semantic intent and scope receipts plus producer lanes
by workspace. A bounded scan of the canonical issue-event source runs before a
new append, after a producer duplicate, and after an append transport failure
whose durability is unknown. Catch-up reads use one message per batch so a
recovered receipt retains the original native offset even after later events.

New commands append with the observed source head as `expectedOffset`. A loser
rescans and rebuilds its sequence, up to eight attempts; exhaustion is the typed
`CommandContention` response with HTTP 409. The recovery scan is deliberately
bounded to 512 batches and 10,000 events in A3.

The SQLite migration replaces only Slice 1's command-receipt table, whose rows
lack canonical intent. Accepted commands remain recoverable from the canonical
source. State tables and transactions are application-owned; A4's generic
operator store, indexes, history, checkpoints, and migrations are not included.

## Server architecture

Effect-first, in the shape `issue-tracker-projections` established. Every
application operation is a description with declared services and typed errors;
`server/local.ts` is the only executable edge.

```text
server/errors.ts        typed failures (Schema.TaggedError)
server/config.ts        AppConfig, read through Effect Config
server/streams.ts       Streams — the protocol-client boundary
server/commands.ts      CommandProducers, the commandId producer lane
server/store.ts         IssueStore + the memory layer
server/store-sqlite.ts  the SQLite layer: one transactional advance
server/maintenance.ts   suffix -> decode -> engine -> commit -> publish
server/sink.ts          the stateSink runtime
server/sink-http.ts     the sink route: scope, native offset, fallback
server/gateway.ts       the Durable Streams HTTP surface the route borrows
server/application.ts   command and query workflows
server/router.ts        the trust boundary: decode, call, translate by _tag
server/runtime.ts       the application Layer, assembled once
```

## Semantics worth knowing

- **Exact acknowledgement.** Every command carries a `commandId`. It is the
  event id, and it derives the producer lane on the issue-events stream. A
  retried command appends nothing and reports the _original_ offset with
  `reconciled: true`. Two independent mechanisms produce that answer: a durable
  command receipt, and — if the receipt were ever lost — the protocol's own
  producer-lane reconciliation.
- **Atomic advance.** Maintained rows, reducer state and the source checkpoint
  commit in one SQLite transaction. A crash can leave the view behind the
  source; it cannot leave it half-folded or ahead of it.
- **Publication is separate from the checkpoint.** The store tracks a published
  position as well as a checkpoint. If publication fell behind — a process died
  between committing and appending — the sink is rebuilt from the durable rows
  rather than replaying messages nobody recorded. Durable rows are the
  authority.
- **Deterministic order.** The engine sorts by the declared `order` expression
  with a stable sort over durable stream order, so replaying a suffix converges
  on the same rows regardless of arrival order.
- **Typed restore.** Durable rows are stored as JSON and decoded through the
  declared `IssueRow` schema on the way out. A row that no longer decodes is a
  typed `StoreRestorePoison` and a `500`, never a served row.
- **Source numbering is durable.** `sequence` continues from the highest folded
  event, not from a counter the process happened to hold.
- **Resume uses the transport cursor.** Every Durable Streams response carries
  `stream-next-offset`. Presenting that value as `?offset=` replays exactly the
  missing suffix. If retained history no longer contains the offset, the sink
  returns `409 resume-unavailable` with `fallback: "snapshot-then-live"`, and
  the browser binding retries from `-1`.

## Local development

From the repository root:

```bash
bun install --frozen-lockfile
bun run build                                   # workspace packages
bun run --cwd examples/issue-tracker build      # browser assets
bun run --cwd examples/issue-tracker dev        # http://localhost:8788
curl -X POST http://localhost:8788/api/workspaces/main/seed
open 'http://localhost:8788/?workspace=main'
```

By default the host keeps the durable log and the maintained state in memory.
Set `ISSUE_TRACKER_DATA=<directory>` to put both on disk, which is what the
restart evidence uses:

```bash
ISSUE_TRACKER_DATA=/tmp/issue-tracker bun run --cwd examples/issue-tracker start
```

## Checks

```bash
bun run --cwd examples/issue-tracker typecheck
bun run --cwd examples/issue-tracker test
bun run --cwd examples/issue-tracker build
bun run --cwd examples/issue-tracker smoke:http
```

`smoke:http` starts a real server on SQLite, drives the slice over the network,
restarts the host against the same databases, and checks native offset resume
and suffix replay with the ordinary Durable Streams client.

## Deliberate deviations from the draft API

Each of these is a place where the drafted DSL could not be implemented as
written against the installed packages, or where implementing it as written
would have been dishonest.

| Draft                                                   | Implemented                                                   | Why                                                                                                                                                                                                                                     |
| ------------------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `x.row.title` on one untyped `x`                        | `selectors<Row, Event, State>()` returning typed references   | An untyped index signature yields `T \| undefined` under `noUncheckedIndexedAccess`, and typing the scopes makes a renamed field a compile error instead of a fold-time one                                                             |
| `evolve: { IssueCreated: { … } }`                       | `evolve: { IssueCreated: (x) => ({ … }) }`                    | The builder runs once at declaration time and returns the same inert record, but it lets each branch read _its own_ event type — `IssueStatusChanged` has no `title` and now cannot reference one                                       |
| `occurredAt: Schema.DateTimeUtc`                        | ISO-8601 string, pattern-checked                              | The same value crosses the event stream, a SQLite column, the Durable State wire and a TanStack DB row; a string keeps all four identical                                                                                               |
| `params: { workspaceId: x.route.workspaceId }`          | `params: ["workspaceId"]`                                     | A `route` expression scope would have exactly one legal shape; listing names keeps the expression vocabulary to scopes that can be evaluated                                                                                            |
| `durableStateCollection(sink, { database: durableDb })` | caller-constructed `DurableStream` passed to `createStreamDB` | The installed `@durable-streams/state` has no caller-supplied database parameter; the caller-owned object it accepts is the stream handle. Ownership stays explicit and the sink contract still supplies route, scope, wire tag and key |
| composite `RowKey` in the spike IR                      | `RowKey = string`                                             | Keeps the SQLite primary key, the Durable State message key and the TanStack DB collection key one value with no encoding step                                                                                                          |

## Evidence limits, stated precisely

- **The browser does not resume across a page reload.** The caller-owned local
  database in this slice is in-memory TanStack DB, so a reload is a fresh
  snapshot-then-live read — which is correct, because there is no retained
  local state for a suffix to be applied to. Native offset suffix replay is
  exercised against the real route by `test/sink-protocol.test.ts` and
  `smoke:http`. Persisting the local replica so a reload can resume is follow-up
  work.
- **The snapshot re-publication does not emit `reset`.** This relation has no
  exits, so a complete set of upserts is already a complete rebuild. It is also
  a workaround: `@durable-streams/state` calls TanStack DB's `truncate()`
  without an open sync transaction when a `reset` is the first thing a session
  sees, which throws. `reset` belongs to the first view that can drop a row, and
  that upstream path needs fixing before then.
- **Receipt recovery is bounded.** A workspace with more than 10,000 canonical
  issue events returns `command-recovery-exhausted` until a later indexed receipt
  authority replaces the A3 scan.
- **`out-of-window` is reachable but not exercised.** The sink maps an offset
  it can no longer serve to the declared snapshot fallback. Nothing in this
  slice trims history, so no test produces that reason.
- **No deployment.** Durable Objects, Alchemy, R2 snapshots and the Cloudflare
  host are out of scope for slice 1 and are not present in this example.
