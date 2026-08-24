# issue-tracker

The Streamsy issue tracker. This is slice 1 of the incrementally maintained
view DSL: one declaration carried end to end, from an HTTP command to a live
React board.

```text
HTTP issue command
-> canonical issue event append (producer lane = commandId)
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
  key: x.row.eventId,
  order: x.row.sequence,
  partitionBy: x.row.workspaceId,
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

Everything it builds is frozen, inert data. `views/dsl.ts` lowers it to a
serializable `RelationPlan`, `views/plan.ts` hashes that plan canonically, and
`views/engine.ts` is a pure interpreter of the plan. `GET /health` reports the
plan hash, so two hosts can be compared by inspection.

`views/contracts.ts` adapts the `@streamsy/views-ir` draft from the
`contracts-spike-minimal` stream (commit `11742f6`), narrowed to the vocabulary
this slice executes. Nodes the slice does not run — filter, project, key, left
join, grouped aggregate, top-N — are deliberately absent rather than declared
and unimplemented.

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
server/sink.ts          the stateSink runtime and its resume tokens
server/sink-http.ts     the sink route: scope, resume, fallback
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
- **Resume is a declared contract.** The sink mints a signed, expiring resume
  token on every response (`x-streamsy-resume`). Presenting it replays exactly
  the suffix. A token that is malformed, expired, or minted for another
  workspace is a `409 resume-expired` carrying `fallback: "snapshot-then-live"`,
  and the browser binding takes that fallback by itself.

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
restarts the host against the same databases, and checks the sink's resume,
suffix and expiry behaviour with the ordinary Durable Streams client.

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
  local state for a suffix to be applied to. Resume, suffix replay and the
  expiry fallback are exercised against the real route by
  `test/sink-protocol.test.ts` and `smoke:http`, including through the browser's
  own binding. Persisting the local replica so a reload can resume is follow-up
  work.
- **The snapshot re-publication does not emit `reset`.** This relation has no
  exits, so a complete set of upserts is already a complete rebuild. It is also
  a workaround: `@durable-streams/state` calls TanStack DB's `truncate()`
  without an open sync transaction when a `reset` is the first thing a session
  sees, which throws. `reset` belongs to the first view that can drop a row, and
  that upstream path needs fixing before then.
- **Concurrent commands can propose the same `sequence`.** Numbering is read
  from the last committed fold, and the maintenance pass runs inside the command,
  so sequential commands are strictly ordered. Two genuinely concurrent commands
  can both read the same next value; the durable stream order still decides the
  fold, because the engine's sort is stable over it. Assigning `sequence` from an
  append acknowledgement is the fix, and it belongs with the multi-writer work.
- **`out-of-window` is reachable but not exercised.** The sink maps a stream
  offset it can no longer serve onto the same typed failure as an aged-out
  token. Nothing in this slice trims history, so no test produces that reason.
- **No deployment.** Durable Objects, Alchemy, R2 snapshots and the Cloudflare
  host are out of scope for slice 1 and are not present in this example.
