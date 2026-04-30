# Streamsy Issue Tracker Demo

A small sync-engine demo for the Part 2 article idea: use Streamsy as the append-only transport while sharing the official Durable State schema, materialization helpers, and StreamDB/TanStack DB client path.

## What it demonstrates

- **Backend**: one Bun server with separated `src/server/` concerns for API routing, Streamsy stream serving, static serving, and materialized state.
- **Transport**: Streamsy (`@streamsy/core` + `@streamsy/storage-memory`) serves `/streams/session/main` from the Bun server.
- **Server state**: `MaterializedState` from `@durable-streams/state` is the canonical in-memory database; mutation handlers append an event to Streamsy and then apply that same event to the materialized state.
- **State events**: each project, issue, or comment mutation appends a Durable State JSON event:

  ```json
  {
    "type": "issue",
    "key": "issue_123",
    "value": { "id": "issue_123", "title": "..." },
    "headers": {
      "operation": "upsert",
      "txid": "...",
      "timestamp": "..."
    }
  }
  ```

- **Frontend**: React + Vite.
- **Client DB**: `createStreamDB` from `@durable-streams/state` builds StreamDB collections (`projects`, `issues`, `comments`) directly from the Streamsy durable stream. There is no bootstrap snapshot endpoint or custom stream-materialization adapter.
- **Mutations through API**: UI posts mutations to the Bun API; appended stream events then flow back into StreamDB/TanStack DB collections through the durable stream.

## Run it

From the repository root:

```bash
bun install
bun --cwd examples/issue-tracker-demo run dev:api
```

In another terminal:

```bash
bun --cwd examples/issue-tracker-demo run dev:web
```

Open the Vite URL (default `http://localhost:5174`). Vite proxies `/api` and `/streams` to the Bun server on port `1338`.

To serve the built React app from the Bun server instead:

```bash
bun --cwd examples/issue-tracker-demo run build
bun --cwd examples/issue-tracker-demo run start
```

Then open `http://localhost:1338`.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/projects` | Create a project and append a `project` state event. |
| `POST /api/issues` | Create an issue and append an `issue` state event. |
| `PATCH /api/issues/:id` | Update issue title/status and append an `issue` update event. |
| `POST /api/comments` | Create a comment and append a `comment` state event. |
| `/streams/session/main` | Streamsy durable stream endpoint. |

## Why this shape

This version intentionally avoids snapshots and prehydration. The demo keeps Streamsy as the server-side durable stream implementation while using the official Durable State tools where they are useful: shared `src/state-schema.ts` types, `MaterializedState` on the server, and StreamDB on the browser client.
