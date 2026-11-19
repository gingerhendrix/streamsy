# Yjs editor on Cloudflare

A small collaborative ProseMirror editor using the `YjsProvider` from
**`@streamsy/y-durable-streams`**, Streamsy's fork of the upstream
`@durable-streams/y-durable-streams` 0.2.9 client. Open a room in two windows to
share text, selections, cursors and names. The three rooms are fixed in the
frontend; the protocol accepts other document paths too.

What it shows: every document is a set of plain Streamsy streams in one SQLite
Durable Object, served to an upstream-compatible Yjs client, with a `@streamsy/projection` fold
that compacts the update log from the object's alarm. Each room's raw update
stream is linked from the page and readable with `curl`.

## Limits

- **8 MiB of updates per document.** Past it, document `POST` returns `413`
  with code `DOCUMENT_TOO_LARGE`; reads keep working. Compaction holds the
  whole document in the object's memory, and the perf run measured clean
  snapshots up to 11 MB but failures from about 12 MB (fragmented) and 40 MB
  (large inserts). The count comes from the compaction fold, so it lags by the
  updates the alarm has not folded yet: one write may land past the budget.
- **Unauthenticated demo.** Anyone with the URL can create, edit and delete
  documents. The backing streams under `/v1/stream` are read-only.

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
loading, snapshot loading with browser `fetch` redirect semantics, partial
compaction state and snapshot discovery after process restart,
a document past 2 MB, rejection of an oversized update, the 8 MiB budget, the
read-only backing streams, concurrent compactions, continued editing after a peer
disconnects, and awareness.

## Architecture

```text
ProseMirror + y-prosemirror
        │ Y.Doc / Awareness
YjsProvider fork (HTTP writes, binary SSE live reads)
        │ /v1/yjs/:service/docs/:docPath        /v1/stream/... (read-only)
Worker: native fetch → one DocumentObject per document
        │ Effect HttpRouter: Yjs routes + Http.routes(streams, { access: "read" })
Streamsy declared streams + DurableObjectStorage (SQLite)
        └─ alarm: ProjectionWakeup → Projection.fold → snapshot
```

| File                   | Contents                                                                  |
| ---------------------- | ------------------------------------------------------------------------- |
| `worker/index.ts`      | Native entry: sends each document and backing stream to its object        |
| `worker/streams.ts`    | The four stream declarations, which own the id grammar, and `forward`     |
| `worker/placement.ts`  | `Placement.byRoute`: every stream of a document goes to the same object   |
| `worker/document.ts`   | Document `PUT`/`GET`/`HEAD`/`POST`/`DELETE`, snapshot discovery and reads |
| `worker/awareness.ts`  | Named awareness streams with `Stream-TTL`                                 |
| `worker/compaction.ts` | The compaction fold                                                       |
| `worker/objects.ts`    | `DocumentObject`: storage layer, routes and the error edge                |

The streams of one document, readable under `/v1/stream/yjs/:service/docs/:docPath/`:

| Stream                         | Contents                                                                                  |
| ------------------------------ | ----------------------------------------------------------------------------------------- |
| `.updates`                     | Append-only lib0 length-prefixed Yjs updates; a producer batch can contain several frames |
| `.snapshots/<offset>_snapshot` | Raw `Y.encodeStateAsUpdate` bytes in 1 MiB messages, without lib0 framing                 |
| `.awareness/<name>`            | Lib0-framed awareness updates, with `Stream-TTL: 3600`                                    |
| `.awareness-index`             | JSON entries `{ name, createdAt }` for named awareness streams, used by deletion          |

Document `PUT` creates `.updates` and the default awareness stream with typed
writer calls. Document `POST`, update reads and awareness reads and writes run the
base `Http.app` program on the declared stream's wire, so producer sequencing,
`maxMessageSize`, framing, live modes and protocol errors come from Streamsy. An
append keeps only the producer headers: the provider sends `Stream-Closed`
when it disconnects, and that must not close the shared document. `live=true`
selects long-poll for documents and SSE for awareness.

Awareness `POST` creates the stream first. Create is idempotent and recreates a
stream whose TTL expired. Presence is ephemeral and not replayed on joining.
Document deletion removes the updates, the snapshot, every awareness stream and
the fold checkpoint in one transaction.

`GET ?offset=snapshot` returns a `307` to `<offset>_snapshot`, or to `-1` before
the first snapshot, with `Cache-Control: private, max-age=5`. A snapshot read
returns one Yjs update and the next offset in `stream-next-offset`.

## The compaction fold

`worker/compaction.ts` declares one **`@streamsy/projection`** fold for all
documents, with the `document` param from the `Updates` stream id. Its state is
`bytes` (every update byte folded), `pending` (bytes since the last snapshot) and
`snapshot` (that snapshot's offset). The checkpoint stays a few bytes for any
document size, and it is also where the budget and snapshot discovery read from.

Compaction runs **off the request path**. `DocumentObject` passes
`ProjectionWakeup.attach(compaction)` to `StreamsyObject` as a wakeup:

1. A write commits to `.updates`. In the same transaction, a commit hook writes a
   pending mark for that document's fold binding.
2. The request returns its status. The host arms the object's alarm.
3. The alarm runs the fold under `Commit.transaction`. When `pending` reaches
   `COMPACTION_THRESHOLD`, the step loads the previous snapshot into a `Y.Doc`,
   applies every update after it up to the log's tail, and writes the result as
   a new snapshot at the tail offset.
4. The snapshot, the removal of the previous one and the fold state commit in
   one SQLite transaction.

The step reads its input stream to the tail, outside the fold's cursor. The fold
is a durable, exactly-once trigger with a counter, and the merge is a side read in
the same transaction. Updates that a snapshot already covers count towards `bytes`
but not towards `pending`, so a backlog does not merge the document again for each
update.

The pending mark is durable: a restarted object reads it and arms the alarm again.
A failed compaction retries with backoff, at most five times, and never fails a
write. Durable Object SQLite rejects a value over 2 MB, so the snapshot is written
as 1 MiB messages and update bodies over 1.5 MiB get `413`. Update history is
kept for stale readers; compaction reduces fresh-client transfer, not disk use.

## Measured on Cloudflare

From one London client, before the budget existed:

- Peer-to-peer edit p50 66 ms; a small `POST` p50 92 ms, of which 2 ms is object CPU.
- Two Bun providers connect and sync in 704 ms; Chromium shows "Live" in 1.25 to 1.7 s.
- 24 peers on one document: 35 to 55 writes per second.
- Documents to 21 MB: snapshot in under 1 s, fresh load in under 1 s.
- Failures (object resets, truncated reads) started at about 12 MB of fragmented
  edits and 40 MB of large inserts. The 8 MiB budget sits below both.

## Upstream conformance

Run against local workerd with a 1,500-byte threshold, using the upstream
`yjs-conformance.test.ts` (commit `461b4026`): **59 passed, 6 failed, 3 skipped (68)**.
The 3 skipped tests start, spy on or restart the upstream Node server. The failures:

| Test                                                                          | Cause                                                                         |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `snapshot-subscription.lifecycle`, `.delivery`, `.invalid-filter`, `.service` | No `__ds` subscription API: `404`                                             |
| `awareness.post-auto-creates` (TTL expiry)                                    | Fakes expiry with a raw `DELETE`, which the read-only mount refuses (`405`)   |
| `delete.doc-cascades-legacy-awareness-index`                                  | Seeds a legacy registry with a raw `PUT` (`405`); this server never wrote one |

## Deployment and other limits

`alchemy.run.ts` follows folder-sync's Worker + SQLite Durable Object + static
assets stack. `bun run --cwd examples/yjs-editor deploy` builds both bundles and
deploys the selected stage with Alchemy.

- No snapshot subscription webhooks, AI peer, room registry or offline persistence.
- The fork fixes browser snapshot discovery only. The upstream 0.2.9 client, and so the fork, has [issue #413](https://github.com/durable-streams/durable-streams/issues/413): edits across reconnect can be lost because the producer epoch is reused.
  The UI pauses editing while disconnected; reload to reconnect.
- Fonts are loaded from Google Fonts, with local serif/sans-serif fallbacks.
