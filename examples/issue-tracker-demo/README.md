# Streamsy Issue Tracker Demo

A small application-level demo: use **Streamsy** as the append-only durable transport for an
issue tracker, while sharing the official **Durable State** schema, materialization helpers, and
the **StreamDB / TanStack DB** client path. The app shows project/issue/comment state, with
optimistic local writes, without writing a bespoke sync engine.

The demo is built around **shareable links for multi-user sync**: click "New shared workspace" to
get a fresh workspace with its own durable stream and a URL (`?w=<id>`) you can copy and share.
Anyone who opens that link syncs to the same workspace stream live — no accounts, no rooms
machinery, just a stream per workspace.

## The Durable State + StreamDB story

```text
   browser form            Bun API server                 Streamsy stream            browser StreamDB
  (optimistic write)  (stateless: materialize/request)   (durable transport)         (TanStack DB view)
        │                       │                              │                            │
        │ POST /api/w/:ws/issues│                              │                            │
        ├──────────────────────▶│                              │                            │
        │                       │  1. materialize workspace ◀──┤  /streams/workspace/<id>   │
        │                       │     state from the stream    │                            │
        │                       │  2. validate + build Durable │                            │
        │                       │     State ChangeEvent        │                            │
        │                       │  3. CAS append at the        │                            │
        │                       │     materialized head ──────▶│  (retry on conflict)       │
        │  201 { txid, offset } │                              │   live read (long-poll) ──▶│ createStreamDB
        │◀──────────────────────┤                              │                            │ replays events
        │                       │                              │                            │ into collections
        │   awaitTxId(txid) reconciles the optimistic write once the event round-trips ─────▶│
```

The **same Durable State `ChangeEvent`** is the unit of truth on both sides:

- the server appends it to the workspace's Streamsy stream;
- the browser replays it from the stream into StreamDB / TanStack DB collections.

There is no snapshot endpoint, no custom stream-materialization adapter, and no server-side
database — the durable stream _is_ the database log, for the server too.

## Shareable workspaces

- **Known workspace**: `main` is the one hardcoded workspace — always present, seeded with demo
  data at boot, and the default when no `?w=` param is set.
- **Shared workspaces**: `POST /api/workspaces` creates a fresh workspace stream with a
  server-generated random id (10 base36 chars) and seeds a "Getting started" project. The client
  navigates to `?w=<id>`; the URL is the shareable artifact.
- **Existence = stream existence**: a workspace exists iff its stream
  (`/streams/workspace/<id>`) exists. There is no registry and deliberately no way to enumerate
  workspaces — a link is unguessable-ish by construction. That is a property of the design, not a
  security boundary; this is a demo.
- **Multi-user sync needs nothing new**: every client on the same `?w=` subscribes to the same
  stream and folds the same events. For a local dev server, "second browser or incognito window"
  is the honest multi-user story; sharing across machines requires the server to be reachable by
  the other user.
- **Memory storage caveat**: the demo uses Streamsy's in-memory storage, so shareable links die
  with the server process. Swapping in the SQLite adapter would make links survive restarts.

## What it demonstrates

- **Backend**: one Bun server (`server/`) — per-resource API route objects (`server/routes/`),
  workspace creation (`workspaces.ts`), Streamsy stream serving (`streams.ts`), Bun fullstack SPA
  serving (the `public/index.html` route), and per-request state materialization (`state.ts`).
- **Transport**: Streamsy (`@streamsy/core` + the in-memory storage factory) serves one durable
  stream per workspace at `/streams/workspace/<id>` from the same Bun process via
  `createHttpHandler`.
- **Stateless server with optimistic concurrency**: there is no long-lived server-side state.
  Each mutation materializes the workspace's `MaterializedState` by folding its stream from the
  beginning, validates against that fresh state, and appends the new event with Streamsy's
  `expectedOffset` CAS precondition at the materialized head. If another writer (a second user on
  the same link) appended in between, the handler re-materializes and retries — so validation
  provably ran against the exact state each event landed on, even across processes. Retry
  exhaustion returns 409. Materialization is O(events) per request — fine for a demo; snapshots/
  compaction are the real fix at scale. Production writers would also add backoff/jitter.
- **State events**: each project, issue, or comment mutation appends a Durable State JSON event:

  ```json
  {
    "type": "issue",
    "key": "issue_123",
    "value": { "id": "issue_123", "status": "done", "title": "..." },
    "old_value": { "id": "issue_123", "status": "open", "title": "..." },
    "headers": { "operation": "update", "txid": "...", "timestamp": "..." }
  }
  ```

  `upsert` events carry only `value`; `update` events also carry `old_value` for replication.

- **Frontend**: React, bundled and served by Bun's fullstack server (an `index.html` import passed
  to `Bun.serve` routes — no Vite, no separate dev server). The workspace comes from `?w=`;
  switching workspace is a full navigation, which keeps the StreamDB lifecycle trivial.
- **Client DB**: `createStreamDB` from `@durable-streams/state/db` builds StreamDB collections
  (`projects`, `issues`, `comments`) directly from the workspace's durable stream, with optimistic
  `onMutate` actions reconciled by `awaitTxId`.

### Package layout note (`@durable-streams/state` 0.3.x)

`@durable-streams/state` 0.3 split its entry points so framework-agnostic state code does not pull
in TanStack DB:

| Import                      | Used by                                              | Provides                                                |
| --------------------------- | ---------------------------------------------------- | ------------------------------------------------------- |
| `@durable-streams/state`    | server (`server/state.ts`, `shared/state-schema.ts`) | `createStateSchema`, `MaterializedState`, `ChangeEvent` |
| `@durable-streams/state/db` | browser client (`src/db.ts`)                         | `createStreamDB`, `StreamDB`, TanStack DB bindings      |

`@tanstack/db` is now a peer dependency of `@durable-streams/state`, so this demo declares it
directly.

## Project layout

The folder structure follows the [Bun fullstack dev server best practices](https://bun.com/docs/bundler/fullstack):
frontend in `src/`, HTML entrypoints in `public/`, server in `server/` with per-resource route
modules. Code shared by both sides (the Durable State schema and entity types) lives in `shared/`.

```text
issue-tracker-demo/
├── public/
│   └── index.html          # HTML entrypoint, imported by the server as a route
├── src/                    # frontend (bundled by Bun from index.html)
│   ├── components/         # incl. ShareControls.tsx (new workspace + copy link)
│   ├── styles/globals.css
│   ├── utils/format.ts
│   ├── utils/workspace.ts  # ?w= param, navigation, create-workspace API call
│   ├── App.tsx             # keys the db lifecycle on the workspace id
│   ├── db.ts               # StreamDB / TanStack DB collections + optimistic actions
│   └── main.tsx
├── server/
│   ├── routes/             # Bun route objects, one module per resource
│   │   ├── projects.ts     # POST /api/w/:ws/projects
│   │   ├── issues.ts       # POST /api/w/:ws/issues, PATCH /api/w/:ws/issues/:id
│   │   └── comments.ts     # POST /api/w/:ws/comments
│   ├── config.ts           # workspace ids: pattern, generator, stream naming
│   ├── state.ts            # per-request materialization + CAS mutation retry loop
│   ├── streams.ts          # multi-stream Streamsy access + /streams HTTP handler
│   ├── workspaces.ts       # POST /api/workspaces (create + seed starter project)
│   ├── utils.ts            # json/error response helpers
│   └── index.ts            # Bun.serve with routes + development options
├── shared/
│   ├── state-schema.ts     # Durable State schema (zod), used by both sides
│   └── types.ts
└── scripts/http-smoke.ts
```

API endpoints are plain `Bun.serve` route objects — exact and parameterized paths with
per-HTTP-method handlers (`req.params.ws` / `req.params.id` via `BunRequest`) — instead of a
hand-rolled router. Unknown `/api/*` paths return a JSON 404; everything else falls back to the
bundled SPA. In development the server enables `development: { hmr: true, console: true }`, so the
client hot-reloads and browser console logs are echoed to the terminal.

## Run it

From the repository root:

```bash
bun install
bun --cwd examples/issue-tracker-demo run dev
```

Open `http://localhost:1338`. One Bun process serves everything: the React app (bundled on the
fly from `public/index.html`, with HMR in development), the `/api` mutation endpoints, and the
per-workspace durable streams under `/streams/workspace/`. `--hot` also hot-reloads the server
code.

To try the multi-user premise end to end: click **New shared workspace**, **Copy link**, open the
link in a second browser or incognito window, then create an issue in one window and watch it
appear live in the other. The `main` demo workspace is unaffected.

For production-style serving (minified client assets, in-memory caching, no HMR):

```bash
bun --cwd examples/issue-tracker-demo run start
```

## Smoke test

A headless smoke test exercises the full mutation + durable-read contract on the known workspace
(create project, reject an issue with an unknown project, create issue, update status, comment,
then replay the stream), plus the shareable-workspace surface: workspace creation and starter
seeding, isolation between workspaces (cross-workspace references are rejected), unknown/malformed
workspace ids, parallel writes converging through the CAS retry loop with no lost updates, and a
direct `Stream-Expected-Offset` conflict probe over HTTP.

From the repository root:

```bash
bun run smoke:issue-tracker
```

or from this package:

```bash
bun run --cwd examples/issue-tracker-demo smoke:http
```

## Endpoints

| Endpoint                      | Purpose                                                               |
| ----------------------------- | --------------------------------------------------------------------- |
| `POST /api/workspaces`        | Create a shared workspace (server-generated id) → `201 {id}`.         |
| `POST /api/w/:ws/projects`    | Create a project and append a `project` upsert event.                 |
| `POST /api/w/:ws/issues`      | Create an issue and append an `issue` upsert event.                   |
| `PATCH /api/w/:ws/issues/:id` | Update issue title/status and append an `issue` update event.         |
| `POST /api/w/:ws/comments`    | Create a comment and append a `comment` upsert event.                 |
| `/streams/workspace/:ws`      | Streamsy durable stream per workspace (read/live-read, `?offset=-1`). |

Unknown workspaces return 404; malformed workspace ids return 400; mutations that lose the CAS
race four times return 409 with a retry hint.

## Why this shape

This version intentionally avoids snapshots and prehydration. The demo keeps Streamsy as the
server-side durable stream implementation while using the official Durable State tools where they
are useful: shared `shared/state-schema.ts` types, `MaterializedState` folded per request on the
server, and StreamDB on the browser client. The stateless server plus `expectedOffset` CAS shows
the 2026 sync doctrine end to end: read from truth, validate against the exact state you condition
on, and let the stream be the only database.
