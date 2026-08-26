# issue-tracker

The Streamsy issue tracker. It carries one declaration end to end, from an HTTP
command to a live React board — and, since Integration 2, it is the whole local
application rather than one vertical slice: three domains, two checked State
sinks, a stream sink, a document sink, an effect sink, and a cross-workspace
inbox no single partition could serve.

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

## What is on screen

| Surface       | Kind                                               | How it converges                                                        |
| ------------- | -------------------------------------------------- | ----------------------------------------------------------------------- |
| Issue board   | checked `stateSink` + generated binding            | Live: Durable State session, native-offset resume, reset-first fallback |
| Label counts  | checked `stateSink` + generated binding            | Live, the same way. Its own route, collection and fingerprint           |
| Issue labels  | read model over the maintained membership relation | Polled, refreshed after every command                                   |
| Activity      | `streamSink` feed, in arrival order                | Polled                                                                  |
| Summary       | `documentSink` with a declared cache policy        | Polled                                                                  |
| Notifications | `effectSink` outbox state                          | Polled                                                                  |
| Inbox         | the _user_ domain's product, fed by the exchange   | Polled — see "Evidence limits"                                          |

## Label membership

Membership is a **second canonical fact family** on its own durable stream,
`workspaces/{workspaceId}/issue-label-events`, folded by its own reducer into
`issue-tracker.issue-labels` and joined by the `issue-tracker.label-counts` plan.

It is a separate stream because of what the declaration language can express,
not by preference: `reduceByKey` is only available directly on a fact source, so
one stream cannot feed two relations keyed by different things. An issue is
keyed by `issueId`; a membership is keyed by an (issue, label) pair.

```bash
# attach
curl -X POST .../api/workspaces/main/issues/seed-plan/labels \
  -d '{"commandId":"c1","labelId":"bug"}'
# detach
curl -X POST .../api/workspaces/main/issues/seed-plan/labels/detach \
  -d '{"commandId":"c2","labelId":"bug"}'
```

Detaching needs **no Durable State delete**. A detached membership stays in the
relation as `attached: false`, and the label-count plan filters it out before it
joins. Deletes remain decoded and rejected everywhere in this example.

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
  key: "eventId",
  mode: "facts",
});

export const issues = view(
  "issue-tracker.issues",
  { schema: IssueRow, key: "issueId" },
  from(issueEvents).reduceByKey({
    key: "issueId",
    reducer: issueLifecycle,
  }),
);

export const boardIssues = defineStateSink({
  name: "issue-tracker.board-issues",
  from: projectBoard,
  row: { decode: decodeProjectBoardCard },
  route: "/state/workspaces/:workspaceId/issues",
  params: { workspaceId: { decode: decodeIdentifier } },
  collection: { name: "issues", type: "issue" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
});
```

A collection's key is declared once, as a row field name. That declaration
lowers to the plan's key expression, and the sink reads its collection primary
key from the relation it publishes, so the plan, the Durable State wire, and the
generated TanStack DB binding cannot disagree about what identifies a row.

Everything it builds is frozen, inert data. `@streamsy/views` lowers it to a
serializable `RelationPlan`, hashes that plan canonically, and
`views/engine.ts` is a pure interpreter of the plan. `GET /health` reports the
plan hash, so two hosts can be compared by inspection.

`@streamsy/views-ir` supplies A1's published JSON-only RelationPlan v2 contract,
including the full authoring vocabulary and explicit `facts` and `state` source
modes. The application consumes that contract through `@streamsy/views`; it no
longer carries a local public IR or DSL compatibility layer.

## State sources

Projects, users, labels, and workspace metadata use four independent Durable
State streams:

```text
state/workspaces/{workspaceId}/projects
state/workspaces/{workspaceId}/users
state/workspaces/{workspaceId}/labels
state/workspaces/{workspaceId}/metadata
```

`domain/catalog.ts` declares those four collections beside their row schemas.
Each declaration names its wire collection and type and names its key field, and
the schema/type/primary-key table the protocol reader binds is derived from
those declarations rather than restated.

The shared `@streamsy/state` protocol reader validates each State envelope and
decodes its row through the catalog's schema/type/primary-key table. Ingestion
then checks the expected collection, envelope key, and workspace before it
commits current rows with that source's native checkpoint in one
application-store transaction. A bad immutable boundary is fail-stop: rows and
checkpoint remain unchanged. `delete` is decoded and returned as the typed
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
server/sink-http.ts     checked route + native offset capabilities
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
- **The transition feed is atomic with the rows it describes.** It is published
  _from_ the committed change history, which is written in the same transaction
  as the rows, on a producer lane whose sequence is durable. A crash before the
  append leaves the batch owed and the next pass publishes it; a crash after the
  append replays the same sequence and the protocol answers `duplicate`. If the
  change history no longer reaches back to an owed batch, the pass fails
  `transition-history-expired` rather than leaving a hole in the log.
- **The exchange reads a durable source registry.** Every workspace the host has
  opened is registered in the global partition, and each pass reopens a bounded
  number of closed ones, least recently exchanged first. An inbox therefore
  converges whether or not anyone is looking at the workspace feeding it.
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
  returns a typed `409 ResumeRejected` with `recovery: "snapshot-then-live"`.
  The native offset is only a transport cursor.
- **Fallback resets first.** The server emits `reset`, snapshot boundaries and
  authoritative rows. The browser adapter lowers the installed library's
  invalid reset call to same-batch deletes followed by snapshot upserts, so a
  stale local row disappears before the session returns live.

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
bun run --cwd examples/issue-tracker smoke:ui
bun run --cwd examples/issue-tracker app:full
```

`app:full` is the Integration 2 acceptance script. It replays a recorded
workspace by appending canonical facts straight onto the durable streams — no
command path — drives a second workspace with live commands, asserts every
product surface agrees, and re-checks all of it after a whole-host restart.

`smoke:http` starts a real server on SQLite, drives the slice over the network,
restarts the host against the same databases, and checks native offset resume
and suffix replay with the ordinary Durable Streams client.

## Deliberate deviations from the draft API

Each of these is a place where the drafted DSL could not be implemented as
written against the installed packages, or where implementing it as written
would have been dishonest.

| Draft                                                   | Implemented                                                 | Why                                                                                                                                                                                                                               |
| ------------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `x.row.title` on one untyped `x`                        | `selectors<Row, Event, State>()` returning typed references | An untyped index signature yields `T \| undefined` under `noUncheckedIndexedAccess`, and typing the scopes makes a renamed field a compile error instead of a fold-time one                                                       |
| `evolve: { IssueCreated: { … } }`                       | `evolve: { IssueCreated: (x) => ({ … }) }`                  | The builder runs once at declaration time and returns the same inert record, but it lets each branch read _its own_ event type — `IssueStatusChanged` has no `title` and now cannot reference one                                 |
| `occurredAt: Schema.DateTimeUtc`                        | ISO-8601 string, pattern-checked                            | The same value crosses the event stream, a SQLite column, the Durable State wire and a TanStack DB row; a string keeps all four identical                                                                                         |
| `params: { workspaceId: x.route.workspaceId }`          | checked codec map `{ workspaceId: { decode } }`             | The accepted slice has no general route-expression IR. The narrow sink contract keeps exact compile-time parameter names and validates decoded path values without competing with A1's expression contract                        |
| `durableStateCollection(sink, { database: durableDb })` | supplied `DurableStream` plus returned `StreamDB` session   | `@durable-streams/state@0.3.1` accepts a pre-built stream but no pre-existing database. The caller owns that stream, the returned session, resume storage and disposal; the adapter does not claim unsupported database injection |
| composite `RowKey` in the spike IR                      | `RowKey = string`                                           | Keeps the SQLite primary key, the Durable State message key and the TanStack DB collection key one value with no encoding step                                                                                                    |

## Evidence limits, stated precisely

- **Receipt recovery is bounded.** A workspace with more than 10,000 canonical
  issue events returns `command-recovery-exhausted` until a later indexed receipt
  authority replaces the A3 scan.
- **The browser does not resume across a page reload.** The local StreamDB is
  in memory, so a reload has no retained rows to receive a suffix. The adapter
  records the last committed native offset behind `ResumeStore`, but correctly
  starts a fresh snapshot until upstream supports a persistent/injected local
  database. In-session reconnect and exact suffix replay are covered.
- **Reset is lowered, not passed to upstream unchanged.** In
  `@durable-streams/state@0.3.1`, a reset-first event calls TanStack DB
  `truncate()` before `begin()`, which throws `NoPendingSyncTransactionWriteError`.
  The adapter preserves reset-first protocol semantics by translating it into
  deletes for the caller-owned collection's current keys and authoritative
  upserts in one sync batch. The integration test proves stale-row removal.
- **Concurrent commands can propose the same `sequence`.** Numbering is read
  from the last committed fold, and the maintenance pass runs inside the command,
  so sequential commands are strictly ordered. Two genuinely concurrent commands
  can both read the same next value; the durable stream order still decides the
  fold, because the engine's sort is stable over it. Assigning `sequence` from an
  append acknowledgement is the fix, and it belongs with the multi-writer work.
- **Retired history cannot be produced locally.** The example has no retention,
  so `history-unavailable` is mapped but cannot be generated. Invalid offsets
  and protocol incompatibility exercise the same explicit recovery policy.
- **The inbox is polled, not live.** It is served by the _user_ partition, which
  owns an inbox and nothing else — no durable stream storage, so no State stream
  to publish and no session to resume. Giving it one would double that domain's
  storage surface and add a fifth contract fingerprint for a product whose only
  writer is the host's own exchange tick. The browser refreshes it on an
  interval and says so on screen. Recorded in `integration-2-decisions.md`.
- **Both graph products are parameterised at one project.** The board and the
  label counts are maintained at `projectId: "streamsy"`, which is what the
  seeded workspace uses. Parameterising them per request needs one operator
  state per parameter binding, which is scale work rather than assembly work.
- **No deployment.** Durable Objects, Alchemy, R2 snapshots and the Cloudflare
  host are out of scope and are not present in this example.
