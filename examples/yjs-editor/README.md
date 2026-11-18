# Yjs editor on Cloudflare

A small collaborative ProseMirror editor using the **unmodified
`YjsProvider` from `@durable-streams/y-durable-streams` 0.2.9**. Open a room in two
windows to share text, selections, cursors and names. The three rooms are fixed in
the frontend; the protocol accepts other document paths too.

## Run locally

From the repository root, build the workspace packages first:

```sh
bun install
bun run build
bun run --cwd examples/yjs-editor dev:worker
```

Open <http://127.0.0.1:1340>. This builds the browser and Worker bundles, then
runs **Miniflare/workerd with SQLite Durable Objects**, without a Cloudflare
account. State lives in `examples/yjs-editor/.scratch/worker`. Stop with Ctrl-C.
`PORT`, `YJS_WORKER_STATE` and `COMPACTION_THRESHOLD` override the port, local
state directory and compaction byte threshold (default 1 MiB).

For frontend iteration, run `bun run --cwd examples/yjs-editor dev:web` alongside
the Worker, then open <http://127.0.0.1:5180>. Vite proxies `/v1` to workerd.
Restart `dev:worker` after rebuilding its bundles or changing Worker code.

```sh
bun run --cwd examples/yjs-editor typecheck    # browser/test and Worker tsconfigs
bun run --cwd examples/yjs-editor test:worker  # builds assets, runs real workerd tests
bun run --cwd examples/yjs-editor build:web
bun run lint
bun run format:check
```

`YJS_TEST_SCRATCH` puts test state in another directory. Each test host is disposed
and its temporary state is removed. Tests cover convergence, snapshot plus tail
loading, partial compaction state and snapshot discovery after process restart,
a document past 2 MB, rejection of an oversized update, concurrent compactions,
continued editing after a peer disconnects, and awareness.

## Architecture

```text
ProseMirror + y-prosemirror
        │ Y.Doc / Awareness
stock YjsProvider (HTTP writes, binary SSE live reads)
        │ /v1/yjs/:service/docs/:docPath
Cloudflare Worker → one DocumentObject per service/document
        │ Effect HttpRouter: Yjs routes + backing-stream routes
Streamsy services + DurableObjectStorage (SQLite)
        └─ alarm: ProjectionWakeup → Projection.fold → snapshot + index
```

`worker/index.ts` builds an Effect `HttpRouter` web handler once per isolate and
receives Worker bindings through request context. It chooses the object by the
complete document identity, so nested paths and their internal streams share one
owner. The object runs Yjs and backing-stream routes in `StreamsyObject`'s app,
using its storage lifecycle and TTL alarm scheduler. The page and protocol share
an origin, so no CORS middleware is needed.

The Yjs adapter implements document PUT/HEAD/GET/POST/DELETE, snapshot discovery
and named awareness streams. It calls Streamsy readers/writers directly, uses
`Http.read` for live reads,
and runs the base `Http.app` program with a substituted request for writes,
preserving producer sequencing without self-HTTP. Streamsy supplies binary
SSE/base64 delivery and TTL expiry. `live=true` selects
long-poll for documents and SSE for awareness, matching the reference server.
Document POST forwards producer headers and forces `application/octet-stream`.
It deliberately filters base-protocol `Stream-Closed`: the stock provider calls
`producer.close()` on disconnect, which must not close the shared document.

The underlying routes are also exposed, as in the upstream proxy:

| Stream under `/v1/stream/yjs/:service/docs/:docPath/` | Contents                                                                                  |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `.updates`                                            | Append-only lib0 length-prefixed Yjs updates; a producer batch can contain several frames |
| `.index`                                              | JSON snapshot entries `{ snapshotOffset, createdAt, foldedBytes }`                        |
| `.snapshots/<offset>_snapshot`                        | Raw `Y.encodeStateAsUpdate` bytes in 1 MiB messages, without lib0 framing                 |
| `.awareness/<name>`                                   | Lib0-framed awareness updates, with `Stream-TTL: 3600`                                    |
| `.awareness-index`                                    | Durable registry of non-default awareness names, used for cascade deletion                |

PUT creates the document and default awareness stream. Named awareness PUT/POST
requires a parent document; POST re-creates a missing or expired awareness stream.
Providers subscribe with `offset=now`, broadcast periodically and use Yjs's
presence timeout. Presence history is ephemeral and is not replayed on joining.
Document deletion removes updates, snapshots, both current and legacy awareness
registries, named awareness streams and the fold checkpoint. Deleting one
awareness stream leaves the document intact.

`GET ?offset=snapshot` returns a 307 to `<offset>_snapshot`, or `-1` before the
first snapshot, retaining the query parameters and using
`Cache-Control: private, max-age=5`. Snapshot reads return a raw Yjs update and
the upstream protocol's incremented sequence cursor. Streamsy advances the first
component of its offsets for successive messages, so that cursor still precedes
the next update. Regular reads preserve the client's Streamsy cursor unchanged.

## The compaction fold

`worker/compaction.ts` declares one **`@streamsy/projection`** fold for all
documents. `Updates` is a custom stream declaration whose `document` param comes
from the `.updates` stream id. The fold's state is two numbers: `bytes`, the
total of every folded update message, and `published`, its value at the last
snapshot. The checkpoint therefore stays a few bytes for any document size.

Compaction runs **off the request path**. `DocumentObject` passes
`ProjectionWakeup.attach(compaction)` to `StreamsyObject` as a wakeup:

1. A write commits to `.updates`. In the same transaction, a commit hook writes a
   pending mark for that document's fold binding.
2. The request returns its status. The host arms the object's alarm.
3. The alarm runs the fold under `Commit.transaction`. When `bytes - published`
   reaches `COMPACTION_THRESHOLD`, the step loads the previous snapshot into a
   `Y.Doc`, applies every update after it up to the log's tail, and writes
   `Y.encodeStateAsUpdate` as a new snapshot at the tail cursor.
4. The snapshot, its index entry, the removal of the previous snapshot and the
   fold checkpoint commit in one SQLite transaction on the document's object.

The pending mark is durable. If the object stops between a write and its alarm,
the restarted object reads the mark and arms the alarm again. A failed
compaction retries with the projection runner's backoff and never fails a
write. Snapshot discovery only reads `.index`; while a snapshot is pending it
returns the previous one, or `-1`.

Durable Object SQLite rejects a string, BLOB or row over 2 MB. The fold never
stores the document, the snapshot is written as 1 MiB messages, and a binary
read concatenates them. The adapter rejects update bodies over 1.5 MiB with
`413`, so one update message always fits.

An object-layer semaphore serializes Yjs and backing-stream mutations; reads
never hold its permit. The alarm does not take it: the fold commits in its own
transaction, and document deletion runs in one transaction, so the alarm never
sees a half-deleted document. Update history is retained so stale readers can
resume; compaction reduces fresh-client transfer, not log disk usage.

## Upstream conformance

Measured against the native Worker on local workerd with a 1,500-byte threshold,
using `yjs-conformance.test.ts` at upstream commit `461b4026`:
**61 passed, 4 failed, 3 skipped (68 total)**. All 61 applicable document,
awareness, presence, deletion, snapshot, supported compaction, error and path/method
tests pass. This is a different target from the earlier 65/68 result for the
upstream Node proxy backed by Streamsy memory storage.

The four failures all concern the intentionally absent `__ds` subscription API:

| Test                                   | Cause                                                     |
| -------------------------------------- | --------------------------------------------------------- |
| `snapshot-subscription.lifecycle`      | 404 when subscription creation expects 201                |
| `snapshot-subscription.delivery`       | 404 when subscription creation expects 201                |
| `snapshot-subscription.invalid-filter` | 404 when subscription validation expects 400              |
| `snapshot-subscription.service`        | 404 when dotted-service subscription creation expects 201 |

Three tests depend on the Node reference implementation and are not applicable:
`subscription.proxy` starts its own Node proxy, `compaction.concurrent-writes`
spies on that server's private compactor, and `Server Restart` stops/restarts
that Node server. In-repo workerd tests cover concurrent compaction and actual
SQLite-backed Worker/object restart instead.

The copied scratch harness points direct backing-stream assertions at the
Worker, enables legacy awareness cleanup with a raw registry fixture, enables
subscription tests so their failures are visible, and targets the Worker for the
dotted-service subscription assertion. Neither the upstream suite nor its 2,700
lines are vendored into this example. Raw logs, harness changes, rerun instructions
and a browser smoke script live in the build-check scratch folder recorded in
the task handoff.

## Deployment and limits

`alchemy.run.ts` follows folder-sync's Worker + SQLite Durable Object + static
assets stack. `bun run --cwd examples/yjs-editor deploy` builds both bundles and
invokes Alchemy for the selected stage. **Deployment was not run.**

- This is an unauthenticated demo, including its underlying stream routes.
- No snapshot subscription webhooks (`__ds` routes return 404), AI peer, room
  registry or offline persistence.
- The stock 0.2.9 client has upstream [issue #413](https://github.com/durable-streams/durable-streams/issues/413): edits across reconnect can be lost because the producer epoch is reused.
  The UI pauses editing while disconnected; reload to reconnect. This does not
  repair the upstream client or guarantee delivery after network failure.
- In Chromium, `fetch(..., { redirect: "manual" })` returns an opaque redirect
  (status 0), including on the same origin. The stock provider's snapshot
  discovery then falls back to `offset=-1`: browser peers converge by replaying
  updates, without the snapshot transfer optimization. This was measured in the
  browser smoke check. Node/Bun stock providers load snapshots in the focused
  tests. Keeping the requested client unmodified preserves this upstream limit.
- The adapter accepts update bodies up to 1.5 MiB. Compaction holds the previous
  snapshot and the new updates in the object's memory, so document size is
  bounded by the 128 MB isolate and the alarm's CPU limit. Neither was measured.
- Fonts are loaded from Google Fonts, with local serif/sans-serif fallbacks.
