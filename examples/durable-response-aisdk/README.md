# Durable response with AI SDK

A1.4 uses actual `@ai-sdk/react` `useChat`, native `ai` `streamText`, and the published `@durable-streams/aisdk-transport` unchanged. Its Bun API hosts SQLite-backed `@streamsy/serve` in the same process. A paced native `MockLanguageModelV3` supplies ten text deltas without keys or external model calls.

From the repository root:

```sh
bun install --frozen-lockfile
bun run build
bun run --cwd examples/durable-response-aisdk dev:api  # http://localhost:1343
```

In a second terminal:

```sh
bun run --cwd examples/durable-response-aisdk dev      # http://localhost:1344
```

`DURABLE_RESPONSE_PACE_MS=700` on the API makes reload and Stop easier to exercise. The default is 250 ms per provider part. `PORT` chooses the API port. `DURABLE_RESPONSE_DB` chooses the SQLite file (default `.data/responses.sqlite`). Vite proxies `/api`, `/responses` and `/debug`; set `DURABLE_RESPONSE_API` to change the proxy target. The API listens on loopback. This demo has no provider configuration and never reads provider keys.

For production builds, `bun run build` in this example emits the SPA to `dist/` and the Bun entry to `server-dist/`. Run `bun run start` and `bun run preview` in separate terminals. The preview has the same proxy and port as development. The compiled server keeps its default database in this example's `.data/` folder.

## Reload and multiple readers

Submit the prompt, then reload `/r/<id>` midway through the answer or open its URL in a second tab. Each mount uses `useChat({ resume: true })` and `createDurableChatTransport({ reconnectApi: .../replay })`. The recorded stream contains a stable assistant message id and prompt metadata, so each fresh reader reconstructs one assistant answer without calling the model again. The page reads its text and stop reason from the native hook's message state.

**Reload replays from the start.** This published transport does not accept a saved offset in `reconnectToStream` and `useChat` exposes no durable offset or checkpoint. The page labels both facts explicitly. It starts the hook with empty messages; restoring a partial message alongside full replay could duplicate content. There is no Drop/Reconnect control. The protocol-level integration test separately proves suffix continuation from a saved offset.

| Route                      | Purpose                                                             |
| -------------------------- | ------------------------------------------------------------------- |
| `POST /api/chat`           | Native transport body `{ id, messages, ... }` → 201 and Location    |
| `GET /api/chat/:id/stream` | Active generation probe: 200 and Location, or 204                   |
| `GET /api/chat/:id/replay` | Replay probe: 200 for an existing open or closed log, 204 if absent |
| `POST /api/chat/:id/stop`  | `{ by, reason }` → 200 `{ alreadyClosed }`                          |
| `GET /api/chat/:id/status` | Storage-derived existence/closure for the debug panel               |
| `GET /responses/:id`       | Read-only Durable Streams protocol                                  |
| `/streams/responses/:id`   | Full protocol endpoint used by the published server writer          |
| `GET /debug/model-calls`   | Process-local scripted calls and observed provider interruptions    |

One response per id is intentional. A second POST to an existing id returns 409. Start a new response from the footer. The active-only probe becomes 204 when the writer has closed; the separate replay probe allows late readers to read a completed or stopped response. No text or stop metadata comes from the polling status endpoint.

## Stop for everyone

The page POSTs the stop endpoint and keeps consuming its native read. Calling `useChat().stop()` alone would detach only that reader, so the page's Stop action does not call it. The API keeps the model controller independent of the HTTP request, records `{ by, reason, at }` in memory before aborting, and uses the public `toUIMessageStream({ messageMetadata })` callback to attach it to the native abort. The stream contains:

```json
{"type":"abort","reason":"The operation was aborted."}
{"type":"message-metadata","messageMetadata":{"stopped":{"by":"reader-a","reason":"Enough","at":"2026-11-17T00:00:00.000Z"}}}
```

The human reason is stored separately: aborting the native model with a string instead of the normal `AbortError` failed the fit probe. The unchanged published writer drains queued appends and closes. Stop awaits that task before replying. Concurrent stops are serialized, natural finish has one terminal winner, and later stops return 200 `{ alreadyClosed: true }` without changing stored bytes. Two readers and a late reader see the same partial text and stopped metadata from `useChat`.

SQLite preserves recorded output after restart. A stopped or finished log can still be replayed. Graceful shutdown aborts active providers with `by: server` / `reason: Host shutting down` metadata and drains their writers before removing HTTP routes. Active controllers/probes and debug counters are process-local: upstream generation cannot resume after a crash, and Stop on a surviving open log without its provider returns 409 explaining the limitation. This is an unauthenticated, single-process demo; full protocol writes are deliberately exposed on loopback. It has no recovery worker or multi-host coordination.

## Checks

```sh
bun run --cwd examples/durable-response-aisdk typecheck
bun run --cwd examples/durable-response-aisdk build
bun run --cwd examples/durable-response-aisdk test
```

The HTTP proofs use actual SQLite, native AI SDK processing and published transports: saved-offset suffix equivalence; one-call replay; active and closed probes; two native readers, observed provider abort, persistent metadata and late replay; concurrent/repeated stops; natural-finish races; invalid requests and encoded-slash rejection. A separate React test mounts real `useChat`, discards its state mid-answer, mounts a fresh hook and checks one complete assistant message and one model call. Browser smoke covers reload, two readers, Stop, late replay and double stop.

The sibling TanStack variant is blocked at its native stop fit gate: `chat` abort invokes `onAbort` but yields no terminal/stop record, and an event queued there is not flushed. Its published connection also discards non-text metadata in default snapshots. No homemade transport or source convention was substituted. The stream's `transport-fit.md` records versions, measured HTTP statuses and the precise limit. The Effect Atom and Foldkit examples remain in their own sibling folders.
