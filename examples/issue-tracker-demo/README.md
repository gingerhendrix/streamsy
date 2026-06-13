# Streamsy Issue Tracker Demo

A small application-level demo: use **Streamsy** as the append-only durable transport for an
issue tracker, while sharing the official **Durable State** schema, materialization helpers, and
the **StreamDB / TanStack DB** client path. The app shows project/issue/comment state, with
optimistic local writes, without writing a bespoke sync engine.

## The Durable State + StreamDB story

```text
   browser form            Bun API server                 Streamsy stream            browser StreamDB
  (optimistic write)   (owns mutations + state)         (durable transport)         (TanStack DB view)
        │                       │                              │                            │
        │  POST /api/issues     │                              │                            │
        ├──────────────────────▶│                              │                            │
        │                       │  1. build Durable State      │                            │
        │                       │     ChangeEvent (upsert)     │                            │
        │                       │  2. append event ───────────▶│  /streams/session/main     │
        │                       │  3. apply event to           │                            │
        │                       │     MaterializedState        │                            │
        │  201 { txid, offset } │                              │   live read (long-poll) ──▶│ createStreamDB
        │◀──────────────────────┤                              │                            │ replays events
        │                       │                              │                            │ into collections
        │   awaitTxId(txid) reconciles the optimistic write once the event round-trips ─────▶│
```

The **same Durable State `ChangeEvent`** is the unit of truth on both sides:

- the server appends it to Streamsy and applies it to `MaterializedState`;
- the browser replays it from the stream into StreamDB / TanStack DB collections.

There is no snapshot endpoint and no custom stream-materialization adapter — the durable stream
_is_ the database log.

## What it demonstrates

- **Backend**: one Bun server (`server/`) — per-resource API route objects (`server/routes/`),
  Streamsy stream serving (`streams.ts`), Bun fullstack SPA serving (the `public/index.html`
  route), and materialized state (`state.ts`).
- **Transport**: Streamsy (`@streamsy/core` + `@streamsy/storage-memory`) serves
  `/streams/session/main` from the same Bun process via `createHttpHandler`.
- **Server state**: `MaterializedState` from `@durable-streams/state` is the canonical in-memory
  database. Each mutation handler appends an event to Streamsy and then applies that same event to
  the materialized state, so reads and validation always reflect the durable log.
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
  to `Bun.serve` routes — no Vite, no separate dev server).
- **Client DB**: `createStreamDB` from `@durable-streams/state/db` builds StreamDB collections
  (`projects`, `issues`, `comments`) directly from the Streamsy durable stream, with optimistic
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
│   ├── components/
│   ├── styles/globals.css
│   ├── utils/format.ts
│   ├── App.tsx
│   ├── db.ts               # StreamDB / TanStack DB collections + optimistic actions
│   └── main.tsx
├── server/
│   ├── routes/             # Bun route objects, one module per resource
│   │   ├── projects.ts     # POST /api/projects
│   │   ├── issues.ts       # POST /api/issues, PATCH /api/issues/:id
│   │   └── comments.ts     # POST /api/comments
│   ├── config.ts
│   ├── state.ts            # MaterializedState + mutation helpers
│   ├── streams.ts          # Streamsy stream + /streams HTTP handler
│   ├── utils.ts            # json/error response helpers
│   └── index.ts            # Bun.serve with routes + development options
├── shared/
│   ├── state-schema.ts     # Durable State schema (zod), used by both sides
│   └── types.ts
└── scripts/http-smoke.ts
```

API endpoints are plain `Bun.serve` route objects — exact and `:id`-parameterized paths with
per-HTTP-method handlers (`req.params.id` via `BunRequest`) — instead of a hand-rolled router.
Unknown `/api/*` paths return a JSON 404; everything else falls back to the bundled SPA. In
development the server enables `development: { hmr: true, console: true }`, so the client
hot-reloads and browser console logs are echoed to the terminal.

## Run it

From the repository root:

```bash
bun install
bun --cwd examples/issue-tracker-demo run dev
```

Open `http://localhost:1338`. One Bun process serves everything: the React app (bundled on the
fly from `public/index.html`, with HMR in development), the `/api` mutation endpoints, and the
`/streams/session/main` durable stream. `--hot` also hot-reloads the server code.

For production-style serving (minified client assets, in-memory caching, no HMR):

```bash
bun --cwd examples/issue-tracker-demo run start
```

## Smoke test

A headless smoke test exercises the full mutation + durable-read contract — create project, reject
an issue with an unknown project, create issue, update issue status, comment — and then reads the
Streamsy stream to assert each mutation emitted a client-readable Durable State change event.

From the repository root:

```bash
bun run smoke:issue-tracker
```

or from this package:

```bash
bun run --cwd examples/issue-tracker-demo smoke:http
```

## Endpoints

| Endpoint                | Purpose                                                              |
| ----------------------- | -------------------------------------------------------------------- |
| `POST /api/projects`    | Create a project and append a `project` upsert event.                |
| `POST /api/issues`      | Create an issue and append an `issue` upsert event.                  |
| `PATCH /api/issues/:id` | Update issue title/status and append an `issue` update event.        |
| `POST /api/comments`    | Create a comment and append a `comment` upsert event.                |
| `/streams/session/main` | Streamsy durable stream endpoint (read/live-read with `?offset=-1`). |

## Why this shape

This version intentionally avoids snapshots and prehydration. The demo keeps Streamsy as the
server-side durable stream implementation while using the official Durable State tools where they
are useful: shared `shared/state-schema.ts` types, `MaterializedState` on the server, and StreamDB on
the browser client.
