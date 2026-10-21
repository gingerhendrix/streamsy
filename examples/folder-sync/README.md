# Folder sync

A file workspace with a State stream for each folder, a live browser, search,
a sync CLI and an agent filesystem. Reads follow the folder streams. Writes use
a JSON API with HTTP preconditions. A stale edit gets 412. Each file value
carries its version, so a replay gives the same versions.

## Run on Bun

Run from the repository root:

```sh
bun install --frozen-lockfile
bun run build
FOLDER_SYNC_WRITER_TOKEN=local-demo-token FOLDER_SYNC_DB=./folder-sync.sqlite bun run --cwd examples/folder-sync start
```

Open http://localhost:1339. Enter `local-demo-token` in **Writer token** to edit.
Reads are public. Every write needs `Authorization: Bearer <writer token>` on
both hosts. An unset writer token disables writes.

In another terminal, seed the docs through the API:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync seed
```

The seed makes one folder per docs section and `docs/root` for root files.
It includes metadata JSON. Repeating an unchanged corpus adds no versions while
the remote content still matches. The seed sends no precondition, so it can
replace user edits. `--server <url>` selects another server.

Run two mirrors in separate terminals, using fresh directories:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-a" --write docs/root
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-b" --write docs/root
```

Edit `docs/root/tutorial.mdx` in the browser. Both mirrors update. Edit the file
under `mirror-a/docs/root/`; the browser and the other mirror update. Leave a
browser draft open, change the file from a mirror, then save the draft. The
browser keeps the draft and reports a stale edit. **Discard draft and reload**
takes the current file. History reads the folder stream and can open earlier
values. Search opens the version it indexed.

Stop the server, edit a writable mirror, and restart with the same database.
Queued writes resume. Concurrent edits become conflict copies. Stop the CLIs
with Ctrl+C when finished. Their state stays under `.folder-sync`.

`PORT` overrides 1339. `FOLDER_SYNC_DB` persists streams, view checkpoints,
index rows and cursors in SQLite. Blobs live beside it in `<database>.blobs/`. Without
a database path, SQLite is transient and blobs use a scoped disk directory under
`.scratch/`. The memory blob store is for tests.

For frontend development, keep Bun running and start Vite:

```sh
bun run --cwd examples/folder-sync dev:web
```

Open http://localhost:5179. Vite proxies `/api` and `/streams` to port 1339.
`start` builds the browser before launching Bun. Restart it after frontend changes
when using the built page. `/health` reports health.

## Run on local workerd

Use this command instead of the Bun server:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync dev:worker
```

It builds the browser and Worker, then starts Miniflare/workerd on port 1339.
Durable Objects and R2 use local disk state under `.scratch/worker/`.
`FOLDER_SYNC_WORKER_STATE` selects another directory; `PORT` changes the port.
Use the same seed, browser token and mirror commands above.

Each folder object owns its file stream and the checkpoint of its folder view.
The index object owns the folder registry and search. After a commit, a
best-effort ping asks the index to catch up. The hourly cron checks every folder
cursor after a lost ping. The local harness runs the same pass on startup and
each hour. An authenticated `POST /api/admin/wake` runs it immediately.
The Worker checks the writer token at its edge, and each object checks it again.

`build:worker` writes two bundles. `public/worker/index.js` is the deploy
bundle. `public/worker/test-worker.js` adds test switches from
`test/worker/test-worker.ts`, such as dropping folder pings, and only the Worker
tests load it.

Blob routes run in the Worker entry and stream directly to local R2. Uploads
require `Content-Length`; missing length gets 400. The shipped clients supply
it. Bun also accepts streamed uploads without a declared length. These commands
use no remote Cloudflare resources. There is no hosted deployment.

## Two State streams

| URL                                  | Collection | Key        |
| ------------------------------------ | ---------- | ---------- |
| `/streams/folders`                   | `folder`   | `folderId` |
| `/streams/folders/<folderKey>/files` | `file`     | `path`     |

Both use the stock read-only State server. PUT, POST and DELETE get 405. Folder
values contain `{ folderId, title }`. File upserts have this shape:

```json
{
  "type": "file",
  "key": "guide/intro.md",
  "value": {
    "path": "guide/intro.md",
    "version": 41,
    "hash": "<sha256-hex>",
    "size": 8,
    "author": "sync CLI",
    "text": "# Intro\n"
  },
  "headers": {
    "operation": "upsert",
    "txid": "cli-save-12",
    "timestamp": "2026-10-19T12:01:00.000Z"
  }
}
```

A delete has `type`, `key` and headers with `operation: "delete"`. It has no
value or version. Each append contains one change. The server time is for
display only.

The owner assigns the next positive folder sequence to each upsert. Deletes
take no number. The version never goes down after delete or restart.
Compare versions only for equality on the same path in the same folder.
Offsets are read cursors. They never name file versions. The append uses
`expectedOffset` as a write guard.

The owner keeps no head table. Current heads, file metadata and the last version
are a fold of the folder's `Files` stream (`server/folder-view.ts`). A write
folds the saved checkpoint plus the stream tail with `Projection.decide`, decides,
and appends at the offset it read. A lost race folds again and decides again.
After four lost races the write answers `503` with `Retry-After`. The owner saves
the checkpoint on its first write in a process and every 32 writes after that. A
save that fails is tried again 32 writes later. A lagging or missing checkpoint
costs only a longer fold.

The view holds metadata only: version, hash, size, author, txid and the offset
where the change starts. `GET` of an inline file reads that one change from the
stream. A file with a short path and txid costs about 200 bytes of checkpoint.
A Worker keeps the checkpoint in one Durable Object SQLite row, and a row holds at
most 2 MB, so one folder holds about 10,000 such files. Long paths lower that
number. Past it, the save fails and logs a warning, writes still land, and each
read folds a longer tail.

Folder IDs are opaque strings. They may contain slashes. The limits are 100
decoded UTF-16 code units and 249 UTF-8 bytes. In stream URLs, `folderKey` is the
hex of the ID's UTF-8 bytes: `docs` becomes `646f6373`. In API URLs, percent-encode
the whole folder ID as one segment. Paths are relative POSIX paths; encode each
path segment separately. Directories are implicit. Folder rename and delete
are outside this demo.

Bootstrap with `offset=-1`. Consume pages in order, then follow
`Stream-Next-Offset` with `live=long-poll` or `live=sse`. Save that cursor after
applying a response. `Stream-Up-To-Date: true` means the read reached the tail.
A stored `reset` control clears that folder's state; apply the changes after it
in the same response and keep the cursor moving forward. A 410 clears state and
starts a fresh read from `-1`. No production writer currently emits either.

## Write API

JSON write bodies are limited to 512 KiB. Send `Content-Type: application/json`.
A JSON write with another content type gets 415 on both hosts: folder creation,
file commits and file deletes.

| Request                                 | Body                                  | Result                                                      |
| --------------------------------------- | ------------------------------------- | ----------------------------------------------------------- |
| `POST /api/folders`                     | `{ txid, folderId, title }`           | 200; same ID and title is a no-op, another title gets 409   |
| `PUT /api/folders/<id>/files/<path>`    | `{ txid, author, hash, size, text? }` | 200 with `{ version, txid, hash }` and a version ETag       |
| `DELETE /api/folders/<id>/files/<path>` | `{ txid, author }`                    | 200 with `{ txid }`; already absent gives `{}` with no txid |
| `GET /api/folders/<id>/files/<path>`    | none                                  | Content bytes with `ETag: "<version>"`, or 404              |

The path comes from the URL. The owner sets the version. Choose a precondition:

| Header             | Meaning                                      |
| ------------------ | -------------------------------------------- |
| `If-Match: "41"`   | Write only if this path still has version 41 |
| `If-None-Match: *` | Create only if the path has no live file     |
| none               | Last writer wins                             |

A precondition miss gets 412 with the current `{ version, txid, hash }` and its
ETag, or `{ "head": null }` when absent. Delete rejects `If-None-Match`.
`If-Match: *`, weak tags and tag lists get 400. The API supports only one quoted
positive version.

After validating content, the owner checks for the current hash before checking
the precondition. Matching content returns the current head and appends nothing,
even when the supplied version is stale. Retry a network error or 5xx with the
same body and precondition. If another writer changed the head, a conditional
retry can get 412. An unconditioned retry remains last writer wins.

Wait for the txid in the answer to appear on the stream. It can differ from the
request's txid when another writer stored the same content first. A no-op delete
returns no txid because it appends nothing; there is nothing to wait for.

## Blobs

All content has a SHA-256 hex hash. Only valid UTF-8 without NUL, at most
64 KiB, may use `text`. Any content may use a blob reference, including content
that could be `text`. The clients send `text` when they can. The file limit is
64 MiB.

1. `PUT /api/blobs/<hash>` uploads bytes with the writer token and
   `Content-Length`. Success is `200 { "hash": "<hash>" }`. A repeated upload
   writes nothing. Wrong bytes get 400; content above the limit gets 413.
2. Commit the file JSON with `hash` and `size`, omitting `text`. The owner checks
   that the blob exists with that size. Missing content gets
   `409 { "error": "blob-missing", "hash": "..." }`. Upload and commit again.

`HEAD /api/blobs/<hash>` returns 200 with `Content-Length`, or 404. GET returns
the bytes with immutable cache headers. Bun hashes while writing to disk,
syncs the file and directory, and publishes it only after validation. The Worker
streams to R2 with the expected SHA-256. Large bytes stay out of folder objects.
Uploads and commits are separate, so an abandoned upload can leave an orphan.

## Browser

The browser uses one `createStreamDB` for the registry and one for the open
folder. TanStack DB live queries build the tree from these collections. File
upserts replace complete rows, including removal of optional inline text.
An edit appears optimistically, sends `If-Match`, and waits for the answer's
txid through `awaitTxId`. A failed save rolls back the optimistic row and keeps
the draft. The accepted version becomes the next edit's base.

History is a paginated replay of the folder stream filtered by path. Earlier
content comes from the saved value's `text` or immutable hash. Markdown and MDX
display without executing HTML or JSX. Search snippets render as escaped text.
Binary files offer a download. Drafts stay in memory and are lost on navigation
or reload. The writer token is stored in browser local storage.

## Sync CLI

`--folders docs,notes` selects folders. Omit it to discover the registry.
`--write notes` selects writable folders; the default is none. `--token` overrides
the writer token environment setting. Writable folders must already exist.

Files live at `<dir>/<folderId>/<path>`. The exported `sync(options)` accepts a
`folderDirectory(id)` function for another layout. Mappings must be relative,
contained and disjoint. IDs such as `docs` and `docs/api` need a different mapping
or selection when used together. `.folder-sync` is reserved. Comma-separated
arguments cannot select IDs containing commas; the runner accepts arrays.

The CLI keeps one cursor per folder and each path's version, remote hash and
local hash. Startup checks each saved cursor against a HEAD tail. A tail
lexicographically before the cursor stops sync with `HistoryRewound`. Restore
the server history or use a fresh mirror after resolving the restore. No file
version is compared numerically to detect a rewind.

Each folder bootstraps by replay, then stays live. Unchanged local files take
remote content. A local edit uses its observed version with `If-Match`; a new
path uses `If-None-Match: *`. On 412, its bytes become a
`name.conflict-<client>[-n].ext` sibling. The original path takes the server file,
and the sibling syncs as a new file. A delete without an observed version sends
no request. A stale delete keeps the remote file. A reset clears folder state
and consumes the rest of that page; 410 replays from `-1` and reconciles again.

SQLite stores observations, cursors and queued operations. Immutable source
files under `.folder-sync/content/<hash>` keep pending bytes safe across edits
and restarts. The CLI streams uploads and downloads. Retryable failures back
off; 401 and 403 stop the daemon with pending writes retained. The server URL
and selection are bound to the mirror state. Use a new directory to change them.

Read-only edits are backed up under `.folder-sync/rejected/` before restoration.
Existing backups get a numeric suffix. Non-retryable commit failures retain their
source and reason. Inspect those summaries offline:

```sh
bun run --cwd examples/folder-sync sync rejected --dir "$PWD/examples/folder-sync/mirror-a"
```

Symlinks, special files and files over 64 MiB are skipped with a warning.
Editor swap files, common backup names and `.DS_Store` are ignored. The watcher
settles editor bursts for 100 ms; periodic scans cover missed events. Run one
daemon per mirror. Ctrl+C closes watchers, reads and SQLite.

## Agent filesystem

`createAgentFileSystem` in `agent/index.ts` returns a just-bash session, its
filesystem, a Better FS Tools adapter and the shared HTTP core. For example,
from code at the repository root:

```ts
import { createAgentFileSystem } from "./examples/folder-sync/agent/index.ts";

const session = await createAgentFileSystem({
  server: "http://localhost:1339",
  folders: ["docs/root"],
  writable: ["docs/root"],
  token: "local-demo-token",
});
await session.bash.exec("cat /docs/root/tutorial.mdx");
await session.bash.exec("echo note > /docs/root/agent-note.txt");
```

The agent keeps folder maps from the State streams and loads blobs by hash.
Folder discovery uses the registry. `folderDirectory(id)` applies the same
mapping rules as the CLI. Writable folders default to none. Root and synthetic
parents are read-only.

Content writes use the version seen by a read or successful write, or an explicit
version precondition. A write to a path the session never read uses
`If-None-Match: *`, even if listing found it. A 412 becomes `ESTALE`; read again
before retrying. Deletes may use a listed version without reading the bytes.
That version is pinned before refresh, so a stale listing also gets `ESTALE`.
The Better FS Tools adapter advertises atomic writes and compare-and-swap.

Shell globs use the session path snapshot. List again after external edits.
Empty directories are session-only. Links, persistent modes and atomic rename
are unsupported. Handles buffer a whole file. `grep --search <fts-query>` uses
the index for candidates, then verifies matches against file bytes. It reports
possible incompleteness at the API's 100-hit limit. Ordinary grep keeps its
usual stdin and flags. Requests support cancellation and bounded retries.

## Search and listing

`GET /api/ls?folder=docs&prefix=guide/&glob=**/*.md` lists live files. The filters
are optional and combine. Rows contain `id`, `folderId`, `path`, `version`,
`hash`, `size`, `updatedAt` and `author`. The ID is a JSON tuple of folder and
path. Globs match paths within each folder and include dotfiles.

`GET /api/search?q=durable&folder=docs` searches current text. FTS5 syntax is
accepted; a bad query gets 400. Results have folder, path, version and
`{ text, match }` snippet segments. They sort by relevance with a 100-hit limit.
Render snippet segments as text. Deleted files and binary content have no search
text. Blob text is capped at the first 1 MiB. Preparation reads one blob prefix
at a time, so a page does not hold every blob's text in memory.

Both indexes run the same search projection (`server/search-index.ts`) with
`Projection.run`, and each commits its rows with the projection checkpoint. Bun
follows each folder stream in process. On the Worker, the index object reads
each `Files` stream from its folder object over HTTP after a ping or the cron. Search and listing are eventually consistent. A missing blob
keeps file metadata without searchable text and does not stall later changes.

## Local measurements

These are single loopback runs with warm OS caches. They are observations,
not a throughput comparison. The recorded corpus had 33 files in 6 folders.
Timing runs from fresh CLI launch until all destination hashes match.

| Docs bootstrap         | Previous Bun version | State on Bun | State on local Worker |
| ---------------------- | -------------------: | -----------: | --------------------: |
| Wall time              |              0.517 s |      0.476 s |               0.539 s |
| GET body bytes         |              254,248 |      268,760 |     76,585 compressed |
| Decoded body bytes     |              254,248 |      268,760 |               268,760 |
| Completed GET requests |                   34 |           19 |                    19 |
| Peak CLI RSS           |          107,624 KiB |  113,600 KiB |           115,416 KiB |

The previous Bun run used `1617e974`; State on Bun used `8531ead6`; the Worker
used `aac27998`. All Worker bootstrap responses used gzip. Response-body counts
exclude headers. Seeding and index catch-up are outside the interval.

| 32 MiB binary round trip |         Bun | Local Worker |
| ------------------------ | ----------: | -----------: |
| Upload and commit        |     0.426 s |      0.860 s |
| Fresh second client pull |     0.293 s |      0.406 s |
| Total                    |     0.719 s |      1.266 s |
| Writer peak RSS          | 143,680 KiB |  152,328 KiB |
| Reader peak RSS          | 121,048 KiB |  108,336 KiB |

Both transfers verified all 33,554,432 bytes by SHA-256. The Bun server's peak RSS
was 173,240 KiB. The Worker harness RSS is not a workerd memory measurement.

## Checks and limits

```sh
bun run --cwd examples/folder-sync typecheck
bun run --cwd examples/folder-sync test
bun run --cwd examples/folder-sync smoke:http
bun run --cwd examples/folder-sync smoke:worker
bun run --cwd examples/folder-sync test:worker
```

Bootstrap and history replay every retained event. A future State compaction
must keep the highest removed version, or a cold fold of the view can give a
used version again. There is no State compaction,
blob orphan sweep, multipart upload or cache garbage collection. Storage,
pending sources, rejected files and backups have no quota. Old mirror SQLite
formats have no migration; use fresh directories. The same applies to server
state: SQLite databases and Durable Objects from older builds keep rows that no
reader uses (owner records, view checkpoints of version 1, the Worker
`folder_sync_cursors` table). Start the demo with fresh state.

Copies of a folder stream are not supported. A copied change keeps the offset
of its source, so a `GET` of inline text from a copy fails. Restoring a missing blob alone does not reindex an
already consumed change. A later write or index rebuild is needed.

Sync scans hash every selected file. An external editor can save during the
final local read-to-rename window. A delete made while stopped during an
interrupted first install can be restored on restart. Browser bundle size,
full replay and scans suit a small demo corpus. There is no phone cache.

Worker creation spans the registry and folder objects. A crash between them
leaves a listed folder whose creation must be retried. A lost ping can leave
search stale until the hourly pass. R2 checks and restart tests use local
workerd; no remote deployment was tested.
