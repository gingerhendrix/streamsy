# Streamsy Issue Tracker Demo

A small sync-engine demo for the Part 1 article idea: use Streamsy as the durable append-only transport, a State Protocol shaped event stream as the server-to-client replication protocol, and TanStack DB as the client-side materialized database.

## What it demonstrates

- **Backend**: one Bun server with in-memory application state.
- **Transport**: `@streamsy/core` + `@streamsy/storage-memory` serving `/streams/session/main`.
- **State events**: each project, issue, or comment mutation appends a JSON event:

  ```json
  {
    "type": "issue",
    "key": "issue_123",
    "value": { "id": "issue_123", "title": "..." },
    "headers": {
      "operation": "insert",
      "txid": "...",
      "timestamp": "..."
    }
  }
  ```

- **Frontend**: React + Vite.
- **Client DB**: TanStack DB local collections (`projects`, `issues`, `comments`) hydrated from `/api/bootstrap` and kept current by long-polling the Streamsy durable stream.
- **Optimistic writes**: UI writes optimistically into TanStack DB, posts to the Bun API, and displays the returned durable stream offset as the reconciliation token.

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
| `GET /api/bootstrap` | Initial state snapshot plus current stream offset. |
| `POST /api/projects` | Create a project and append a `project` state event. |
| `POST /api/issues` | Create an issue and append an `issue` state event. |
| `PATCH /api/issues/:id` | Update issue title/status and append an `issue` update event. |
| `POST /api/comments` | Create a comment and append a `comment` state event. |
| `/streams/session/main` | Durable Streams protocol endpoint served by Streamsy. |

## Why this shape

This is intentionally the simplest article-demo implementation, not the full session-mirror engine. The server keeps canonical in-memory maps and emits state events directly. The client treats the stream as the durable replication log and materializes it into TanStack DB collections. That keeps the core idea visible: durable offsets provide resumability and an offset can act as the confirmation token for optimistic UI.
