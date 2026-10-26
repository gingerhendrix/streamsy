# Folder sync

A file workspace with a State stream for each folder, a live browser, search,
a sync CLI and an agent filesystem. Reads follow the folder streams. Writes use
a JSON API. Each write names the version it started from, its parent. Concurrent
writes all land: the last one is the head and history keeps every one. Each file
value carries its version and parent, so a replay gives the same history.

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
the remote content still matches. The seed names the current head as its parent,
so it can replace user edits. The user edit stays in history. `--server <url>` selects another server.

Run two mirrors in separate terminals, using fresh directories:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-a" --write docs/root
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-b" --write docs/root
```

Edit `docs/root/tutorial.mdx` in the browser. Both mirrors update. Edit the file
under `mirror-a/docs/root/`; the browser and the other mirror update. Leave a
browser draft open, change the file from a mirror, then save the draft. The save
lands and becomes the head. History shows both versions with the same parent.
**Discard draft and reload** takes the current file instead. History reads the folder stream and can open earlier
values. Search opens the version it indexed.

Stop the server, edit a writable mirror, and restart with the same database.
Queued writes resume. Concurrent edits all land, and the last one is the head. Stop the CLIs
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
The index object owns the folder registry, wake stream and search index. The
Worker checks the writer token at its edge, and each object checks it again.

`ProjectionWakeup.attach(filesWake)` runs from a folder object's
alarm after each local `Files` commit. It appends the folder's current tail to
`FolderWake` through `wakeLink` in `worker/links.ts`. The index object's own
wake commits drive a second attached projection, `wakeSearch`, which reads each
affected folder through the read-only `filesLink`. Both links use internal
`/__` mounts that the Worker edge never forwards. Public streams stay read-only.
Search rows and each folder's source checkpoint commit together; the wake
checkpoint advances only after every affected folder has caught up. A removed
folder produces a wake delete, clears its index rows and forgets its binding.

When the index object starts, it compares durable wake rows with search
checkpoints and catches up dirty folders without another write. It also checks
indexed folders whose wake row disappeared and resumes unacknowledged wake
batches. The pending set in each folder object still lives in memory: a stop
before a source commit produces its wake row can delay that row until the next
folder write. Bun uses `Projection.attach` for both search and wake production;
its startup pass also reads the registry to recover missed source commits.

`build:worker` writes two bundles. `public/worker/index.js` is the deploy
bundle. `public/worker/test-worker.js` adds test switches from
`test/worker/test-worker.ts`, such as pausing index alarms, and only the Worker
tests load it.

Blob routes run in the Worker entry and stream directly to local R2. Uploads
require `Content-Length`; missing length gets 400. The shipped clients supply
it. Bun also accepts streamed uploads without a declared length. These commands
use no remote Cloudflare resources.

## Deploy to Cloudflare

`alchemy.run.ts` declares one Worker, the two Durable Object classes and one R2
bucket. The Worker serves the browser assets. Alchemy
keeps the stack state in `.alchemy/` in this folder. The CLI runs under Node and
uses the default Alchemy profile.

```sh
FOLDER_SYNC_WRITER_TOKEN=<token> bun run --cwd examples/folder-sync deploy --stage <stage>
```

`deploy` builds the browser and the Worker, then deploys. The Worker name is
`folder-sync-<stage>` on `workers.dev`, and the command prints its URL. The token
becomes a Worker secret. Use the URL with the seed, sync and agent commands above.
Run the HTTP smoke once against a fresh stage, because it expects new file
versions:

```sh
FOLDER_SYNC_URL=<url> FOLDER_SYNC_WRITER_TOKEN=<token> bun run --cwd examples/folder-sync smoke:http
```

Remove the stage and its data with `bun run --cwd examples/folder-sync destroy --stage <stage>`.
The destroy command also reads `FOLDER_SYNC_WRITER_TOKEN`.

On Cloudflare, an upload without `Content-Length` (for example
`Transfer-Encoding: chunked`) still succeeds. The edge gives the Worker a declared
length. The 400 applies only on local workerd.

## Three State streams

| URL                                  | Collection | Key        |
| ------------------------------------ | ---------- | ---------- |
| `/streams/folders`                   | `folder`   | `folderId` |
| `/streams/folders/<folderKey>/files` | `file`     | `path`     |
| `/streams/wake`                      | `stream`   | `stream`   |

All three use the stock read-only State server. PUT, POST and DELETE get 405.
Folder values contain `{ folderId, title }`.

The wake stream has one row for each folder `Files` stream. The row holds the
stream id and its tail after its last write:

```json
{
  "type": "stream",
  "key": "folders/646f6373/files",
  "value": { "stream": "folders/646f6373/files", "offset": "0000000000000003_0000000000000000" },
  "headers": { "operation": "upsert" }
}
```

The `files-wake` projection writes the rows (`server/wake.ts`). A row lags its
folder write by one projection pass. A reader that saved a folder cursor
compares it with `offset` and reads the folder only when the row is past it.
Offsets are fixed-width strings, so a string compare orders them. The host
creates the empty wake stream at start, so a reader never gets 404.

File upserts have this shape:

```json
{
  "type": "file",
  "key": "guide/intro.md",
  "value": {
    "path": "guide/intro.md",
    "version": 41,
    "parent": 40,
    "hash": "<sha256-hex>",
    "size": 8,
    "author": "sync CLI"
  },
  "headers": {
    "operation": "upsert",
    "txid": "cli-save-12",
    "timestamp": "2026-10-19T12:01:00.000Z"
  }
}
```

A delete is an upsert of a tombstone with the value
`{ path, version, parent, author, deleted: true }` and no hash. Each append
contains one change. The server time is for
display only. A change holds no content. The content is in the blob store under
`hash`, so every version of every file is addressable by its hash.

The owner assigns the next positive folder sequence to each change, a tombstone
too. `parent` is the version that the writer started from, or `null`. The version
never goes down after delete or restart. A tombstone is a row, so a later State
compaction keeps its version and the high-water mark.
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

The view holds metadata only: version, parent, hash, size, author and txid of
each live path, the tombstone version of each deleted path, and the retry memory
of the last 1024 changes (about 200 KiB with short paths). `GET` of a file looks up the
hash in the view and streams that blob. A file with a short path and txid costs
about 200 bytes of checkpoint.
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

JSON writes need `Content-Type: application/json`. A JSON write with another
content type gets 415 on both hosts: folder creation, file commits and file
deletes. JSON bodies have no size limit.

`POST /api/folders` takes `{ txid, folderId, title }`. The same ID and title is
a no-op, and another title gets 409.

| Request                                 | Body                                   | Result                                                                    |
| --------------------------------------- | -------------------------------------- | ------------------------------------------------------------------------- |
| `PUT /api/folders/<id>/files/<path>`    | `{ txid, author, parent, hash, size }` | 200 with `{ version, parent, txid, hash }` and a version ETag             |
| `DELETE /api/folders/<id>/files/<path>` | `{ txid, author, parent }`             | 200 with `{ version, parent, txid }`; no live file gives `{}`             |
| `GET /api/folders/<id>/files/<path>`    | none                                   | Content bytes with `ETag: "<version>"`, or 404 with `{ error, deleted? }` |

The path comes from the URL. The owner sets the version. `parent` is the version
that the writer started from, or `null` when it saw no file at the path. A parent
after the folder's last version gets 400. The server records the parent and does
not compare it with the head, so every accepted write lands. The last write in
stream order is the head. `If-Match` and `If-None-Match` are not part of the
protocol, and a write that sends one gets 400.

A delete appends a tombstone: an upsert whose value is
`{ path, version, parent, author, deleted: true }`. It takes a version like any
other change. A read of a deleted path gets 404 with `deleted: <version>`, so a
writer can name the tombstone as the parent of a new file. Readers treat a
tombstone as a delete.

The owner applies three rules in this order:

1. **Retry.** The folder view remembers the answers of its last 1024 changes by
   txid. A write whose txid is in that memory gets its first answer again and
   appends nothing, even when other writes landed since. The memory checks the
   path and the kind of change: a txid used again for another path, or for a
   delete after a commit, gets 400.
2. **Same content.** A commit whose hash matches the head returns the current
   head and appends nothing. A delete of a path with no live file returns `{}`.
3. **Append.** Otherwise the change gets the next version and becomes the head.

Retry a network error or 5xx with the same body. A retry that arrives after 1024
other changes in the folder lands again as a new head; its parent shows that it
is old. Two writes with the same parent are a fork. A reader can find a fork
with one pass over the stream: a change whose parent is not the version of the
change before it on the same path.

Wait for the txid in the answer to appear on the stream. It can differ from the
request's txid when another writer stored the same content first. A no-op delete
returns no txid because it appends nothing; there is nothing to wait for.

## Blobs

All content lives in the blob store, under its SHA-256 hex hash. Every write
uploads the bytes first and then commits a reference to them. This applies to
small text and to binary content alike. The file limit is 64 MiB.

1. `PUT /api/blobs/<hash>` uploads bytes with the writer token and
   `Content-Length`. Success is `200 { "hash": "<hash>" }`. A repeated upload
   writes nothing. Wrong bytes get 400; content above the limit gets 413.
2. Commit the file JSON with `hash` and `size`. The owner checks
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
upserts replace complete rows.
An edit appears optimistically, uploads the blob, commits with the draft's base as
its parent, and waits for the answer's txid through `awaitTxId`. A save from an
old base lands and becomes the head. A failed save rolls back the optimistic row
and keeps the draft. The accepted version becomes the next edit's base. One
adapter turns a tombstone upsert into a State delete, so TanStack DB sees the
same rows as for a plain delete.

History is a paginated replay of the folder stream filtered by path. Each entry
shows its parent, and a tombstone shows as a versioned delete. Earlier
content comes from the blob that the saved value's hash names. Markdown and MDX
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

Each folder bootstraps by replay and catches up once. The CLI then holds two
live reads, whatever the number of folders: the registry and the wake stream.
It holds no live read for each folder. A new registry entry starts the folder:
map, initialise and catch up. A wake row for a started, selected folder that is
past its saved cursor catches that folder up from the cursor to its tail. Rows
for folders that are not selected are skipped. A row for a selected folder that
has not started yet waits for the registry, whose start reads to the tail. A
wake delete logs a warning and keeps the mirror. A stored reset in the wake
stream catches up every started folder.

The CLI saves each folder cursor with the changes that it covers, and the wake
cursor (`wakeCursor`) only after every row of the page is handled. A stop
between the two reads the same rows again; the offset check skips the folders
that were already read. A wake 410 catches up every started folder and reads the
wake stream again from `-1`. The wake read uses the same retry as other remote
reads. A server without `/streams/wake` answers 404, which stops the CLI.

Unchanged local files take remote content. A local edit names the version that
its disk copy came from as its parent. A new path names the tombstone version when the path was deleted,
or `null`. Every write lands: a pulled change installs over the local file, and
a local edit that is still queued is sent later and becomes the head. A first
sync that finds a local file with other bytes than the server commits the local
file with parent `null`. The server copy stays in history. A delete without an
observed version sends no request. A reset clears folder state and consumes the
rest of that page; 410 replays from `-1` and reconciles again.

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
parents are read-only. A shell write to a read-only path fails that command
only, as in bash: `echo x > /ro/f` prints `bash: /ro/f: Read-only file system`,
sets `$?` to 1, and the script goes on. The root `patches/just-bash@3.6.0.patch`
gives just-bash this behaviour until a release carries upstream pull request
vercel-labs/just-bash#613.

A content write names the version seen by a read or successful write as its
parent. A write to a path the session never read has parent `null`, even if
listing found it. A delete may name a listed version without reading the bytes.
Every write lands, so a stale read or listing makes a fork in history and does
not fail. An explicit precondition (`absent` or `version`) is checked against a
fresh snapshot before the commit and fails with `EEXIST` or `ESTALE`. That check
is not atomic with the commit. The Better FS Tools adapter advertises atomic
writes and no compare-and-swap.

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

Both hosts use the search projection in `server/search-index.ts`, committing SQL
rows with its source checkpoint. Bun attaches it directly to local `Files`
commits. The Worker attaches `wakeSearch` to the index object's local
`FolderWake` commits, then reads affected `Files` streams through `filesLink`.
At start, the Worker index replays the wake rows after the durable `wakeSearch`
cursor. Bun runs the search binding of each registered or indexed folder once,
because attach keeps its pending work in memory. An index restart needs no new
folder write.
Search and listing are eventually consistent. A missing blob keeps file metadata
without searchable text and does not stall later changes.

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

Restoring a missing blob alone does not reindex an
already consumed change. A later write or index rebuild is needed.

Sync scans hash every selected file. An external editor can save during the
final local read-to-rename window. A delete made while stopped during an
interrupted first install can be restored on restart. Browser bundle size,
full replay and scans suit a small demo corpus. There is no phone cache.

Worker creation spans the registry and folder objects. The folder object creates
the stream before the registry lists the folder. A crash between them leaves an
unlisted empty stream, and a retry of the create lists it. A folder object that stops before producing a wake row can leave search stale
until its next write. Restart and dirty-row catch-up tests use local workerd. One deployed
run on Cloudflare covered the HTTP smoke, seed, sync mirrors, the browser, the
agent filesystem and a redeploy. The attached indexing and startup catch-up
changes have local workerd coverage.
