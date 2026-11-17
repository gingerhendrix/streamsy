# Durable response with TanStack AI

A1.5 uses native `@tanstack/ai-react` `useChat`, `@tanstack/ai` `chat` and `fakeText`, and the published `@durable-streams/tanstack-ai-transport` **0.0.10 with a tracked Bun patch for raw replay**. A Bun API hosts SQLite-backed `@streamsy/serve` in the same process. The native fake adapter provides paced text without keys, network model calls or paid providers.

The root `patchedDependencies` entry applies `patches/@durable-streams%2Ftanstack-ai-transport@0.0.10.patch`. Only `emitSnapshotOnSubscribe: false` consumption changes: it uses the native client’s `jsonStream()` to wait for final-body consumption and propagate body/JSON errors. Previously, `closed` could resolve before the final JSON callback, dropping a late reader’s entire closed log. The default snapshot/batch path and send API are unchanged; that default path retains the upstream race. No global client patch is applied. The owner explicitly authorized this raw-mode change in the stream’s `tanstack-raw-replay-decision.md`.

From the repository root:

```sh
bun install --frozen-lockfile
bun run build
bun run --cwd examples/durable-response-tanstack dev:api  # http://localhost:1345
```

In a second terminal:

```sh
bun run --cwd examples/durable-response-tanstack dev      # http://localhost:1346
```

`PORT` chooses the API port. `DURABLE_RESPONSE_DB` selects SQLite (default `.data/responses.sqlite`). `DURABLE_RESPONSE_PACE_MS` defaults to 150 ms per native fake token, roughly four characters. Set it to 250 for slower manual interaction, or zero to disable pacing. The API listens on loopback. Vite proxies `/api`, `/responses` and `/debug`; set `DURABLE_RESPONSE_API` to change its API target. No provider keys are read.

`bun run build` in this example emits the SPA to `dist/` and the Bun entry to `server-dist/`. Run `bun run start` and `bun run preview` in separate terminals to use both production builds, with the same proxies and ports. The compiled server retains its default database in this example's `.data/` folder.

## Start and replay

The form first creates an empty response log. Then native `useChat().sendMessage(prompt)` sends through the published connection. Preparing first lets that connection subscribe to an existing stream before it sends; its `send` method does not follow Location. Each id permits one generation. Further prompts use New response.

Reload `/r/<id>` midway or open its URL in another tab. Each mount creates a fresh `durableStreamConnection({ sendUrl, readUrl, emitSnapshotOnSubscribe: false })` and passes it to `useChat({ threadId, connection, live: true })`. Native `chat`/`fakeText` supplies text; native `useChat` folds the assistant message. Native RUN_STARTED metadata carries the prompt. The page's `onChunk` observes terminal events and counts received events; `onCustomEvent` decodes the application stop convention below. Polling supplies only storage closure and model counters, never answer text or the stop reason.

**Reload is full raw replay from -1.** The patched raw connection accepts an initial offset but does not advance it from item reads: the native item stream exposes no batch checkpoints. Each subscription starts at the requested initial offset (default -1), including another subscription on the same connection. It provides no evolving checkpoint callback to the hook. The page labels that limit explicitly, creates a fresh connection and empty hook state after reload, and does not fabricate a saved-offset display. It has no Drop/Reconnect control. The low-level protocol integration test separately proves byte-identical suffix reads from a saved offset.

| Route                        | Behavior                                                                               |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| `POST /api/chat/:id/prepare` | Create the empty SQLite log; 201 and Location; 409 if it exists                        |
| `POST /api/chat/:id`         | Native connection body `{ messages, data }`; start native generation; 201 and Location |
| `GET /api/chat/:id/stream`   | Active-only probe: 200 and Location, or 204                                            |
| `GET /api/chat/:id/replay`   | Locate an existing open/closed log: 200 and Location, or 204 if absent                 |
| `POST /api/chat/:id/stop`    | `{ by, reason }` → 200 `{ alreadyClosed }` after durable closure                       |
| `GET /api/chat/:id/status`   | Storage existence/closure for the debug panel                                          |
| `GET /responses/:id`         | Read-only Durable Streams protocol                                                     |
| `/streams/responses/:id`     | Full protocol endpoint used by the published writer                                    |
| `GET /debug/model-calls`     | Native fake adapter call count and observed `chat` onAbort count                       |

The published connection reads the known response URL directly; it does not call the probe endpoints automatically. The active probe becomes 204 after closure, while the replay probe also locates completed/stopped output. Starting requires a prepared empty log; concurrent or later starts to the same id return 409.

## Explicit application stop convention

Native TanStack abort does **not** write a stopped event. The earlier fit check measured that limitation. The owner explicitly authorized this example-local source wrapper in the stream's `tanstack-decision.md`. Generation still uses the native framework. The separately disclosed raw-consumption patch does not implement the stop convention.

Stop POST reserves the terminal decision and records reader identity, reason and time before aborting the native model with `RUN_CANCEL_REASON`. The wrapper continues draining the native iterator until its `onAbort` observes cancellation. It then ends any still-open native text message and yields these native event shapes:

```json
{"type":"CUSTOM","name":"durable-response.stopped","value":{"by":"reader-a","reason":"Enough","at":"2026-11-17T00:00:00.000Z"}}
{"type":"RUN_FINISHED","threadId":"response-id","runId":"response-id","outcome":{"type":"cancelled"}}
```

The UI identifies this as an application CUSTOM convention. `emitSnapshotOnSubscribe: false` is essential: default text snapshots discard custom and terminal events. Raw replay lets current and late readers receive the same event sequence, and native hook callbacks see the same persisted stop. The page keeps consuming after POST instead of calling local `useChat().stop()`, which would detach that client with this published connection.

The published `toDurableStreamResponse` runs in await mode inside a server-owned task so writer failures remain observable. Start returns immediately; Stop awaits that original task, its queued appends and close, and checks durable closure before replying. The server logs task failures. A natural terminal and Stop compete for one synchronous terminal decision. Concurrent stops are serialized; subsequent stops return 200 `{ alreadyClosed: true }` without changing recorded bytes. A stopped native source is suppressed after that decision, so it cannot add a competing terminal. The fake adapter checks its signal between paced tokens; Stop waits for that native observation rather than claiming the timer itself is interruptible.

Graceful shutdown marks active responses stopped by `server` with reason `Host shutting down`, aborts native providers and drains their writers while HTTP routes still exist. SQLite preserves closed responses for replay after restart. A hard crash cannot resume a provider; active controllers/probes and model counters are process-local. Stop on a prepared response with no provider, or an open log left by a crash, returns explicit 409. There is no recovery worker or multi-host coordination.

## Verification and limits

```sh
bun run --cwd examples/durable-response-tanstack typecheck
bun run --cwd examples/durable-response-tanstack build
bun run --cwd examples/durable-response-tanstack test
```

Four HTTP integration tests cover natural finish; one-call full raw replay; saved-offset byte equivalence; probes; identical chunks, stop metadata and closure for two current readers, a late reader and low-level readers; observed native interruption; repeated/concurrent stops; competing starts and finish/stop races; and invalid/prepared/missing ids and bodies. Two React tests use actual `useChat`: native send followed by a fresh mount/connection reconstructs one complete assistant answer with one model call, and current/late callbacks observe identical stopped metadata plus the cancelled terminal. Eight additional runtime regression cases establish close-before-body ordering and verify delayed valid/empty bodies, malformed JSON, body failures, live SSE failures without duplicate unhandled rejection, pending-read abort cleanup, early consumer return and the explicit initial-offset behavior. Production browser smoke passes reload/two current readers, shared Stop, late closed replay with identical partial text and stopped metadata, and double stop as HTTP 200 with `alreadyClosed: true`. The stream’s `verification-tanstack-final.md` records the patched result and screenshots; earlier verification artifacts preserve the original failures.

This is an unauthenticated single-process loopback demo, including its internal full-write route. The default fake produces text only; tool calls, approvals, real providers, durable generation recovery and multi-host coordination are outside scope. The explicit stopped CUSTOM convention is application-specific. Durable Object hosting, batching/access flags, CLI and raw proxy variants are separate work. The existing AI SDK, Atom and Foldkit examples are unchanged.
