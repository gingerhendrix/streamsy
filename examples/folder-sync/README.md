# Folder sync

A durable file workspace with one append-only log per folder, full history, a live
browser, full-text search, and a local sync CLI. Commit order chooses the latest
version. A write that replaces an unseen change records an `overwrote` mark.
The change feed, manifest, and search index rebuild from the folder logs.

## Try it by hand

From the repository root, install and build the workspace packages:

```sh
bun install --frozen-lockfile
bun run build
```

Start a persistent server in one terminal. `start` builds the Vite UI before
launching Bun. Open http://localhost:1339 in your browser.

```sh
FOLDER_SYNC_DB=./folder-sync.sqlite bun run --cwd examples/folder-sync start
```

In another terminal, seed the Streamsy docs through the HTTP API:

```sh
bun run --cwd examples/folder-sync seed
bun run --cwd examples/folder-sync seed
```

The second run adds no versions. The first docs directory becomes the section
folder: `docs/streams`, `docs/projections`, `docs/demos`, `docs/hosting`, and
`docs/http`. Root files live in `docs/root`. The script includes metadata JSON
and uses a stable operation ID made from folder, path, and content hash. Repeating
an earlier seed does not undo subsequent user edits. Changed seed content gets a
new operation ID and replaces user edits without an `overwrote` mark, because
the seed uses the current head as its base. `--server <url>` selects another server.

Start two mirrors in separate terminals. Use fresh directories for this check:

```sh
bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-a" --write docs/root
bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-b" --write docs/root
```

Choose `docs/root/tutorial.mdx` in the browser and open **Edit**. Change a paragraph,
set an author name, and save. Both mirrors update. Edit the same file under
`examples/folder-sync/mirror-a/docs/root/` and watch the browser and the other mirror update. Search
for a distinctive word from the edit. Search results open the exact version they
indexed. **Open current** returns to the latest file.

To see an overwrite mark, leave an unsaved draft in the browser, edit the file
in a mirror, wait for the server change, then save the draft. History says
“replaced a change you did not see”. Its link opens the replaced version. Save
again from the browser; this save uses the first receipt as its base and does
not mark that first save as unseen.

Stop the server, edit a writable mirror, and restart the server with the same
SQLite path. Queued writes deliver and both mirrors converge. The browser shows
**Offline** while its stream reader cannot connect. Returning online or bringing
the page into view immediately retries a sleeping network read. Without those
events, network retries back off from 100 ms to a 30 s ceiling; HTTP errors retain
the stream client's backoff and any server `Retry-After` delay. Stop both CLIs with Ctrl+C
when finished. Their state stays under `.folder-sync` in each mirror.

## Browser and development

The folder tree uses a TanStack DB collection fed by `/streams/manifest` through
`@durable-streams/state/db`. It loads every retained event, then follows new
ones. Clean viewers refresh on manifest changes. An unsaved draft retains its
open version and shows when a newer head arrives. Historical views are read-only.
The browser editor supports UTF-8 text. Markdown and MDX show Markdown without
executing HTML or JSX. Other files offer a raw download; blob files download by
immutable hash. Search snippets are text segments with boolean match flags,
rendered as escaped React children. Empty, unsafe, and relative Markdown links
are plain text; absolute HTTP, HTTPS, and mail links open a new tab. An author name is a display label.

For browser development, run the server and the Vite client separately:

```sh
bun run --cwd examples/folder-sync start
bun run --cwd examples/folder-sync dev:web
```

Open http://localhost:5179. Vite proxies `/api` and `/streams` to port 1339. The
Bun listener serves the built HTML and its assets, with API and stream routes
falling through to the Effect router. It uses the same scoped shutdown as the
issue tracker. `/health` is a small health response. Rebuild the UI or restart
`start` after frontend changes; `dev:web` provides live frontend development.

```sh
bun run --cwd examples/folder-sync test
bun run --cwd examples/folder-sync smoke:http
bun run --cwd examples/folder-sync typecheck
```

## File API and storage

The default port is 1339 (`PORT` overrides it). An unset or empty `FOLDER_SYNC_DB`
uses transient SQLite (`:memory:`), including FTS5. Setting a path persists logs,
checkpoints, blobs, and search in the named SQLite file. The folder service also
has a MemoryStorage test host; the indexed server requires SQL transactions.
Tests use a scoped directory under `.scratch/`; `FOLDER_SYNC_SCRATCH` overrides it.

Create a folder with `POST /api/folders` and JSON `{ "folderId": "docs", "title": "Docs" }`.
Folder IDs are opaque strings limited to 100 decoded UTF-16 code units and
249 UTF-8 bytes. The router counts decoded units, so percent encoding slashes
or Unicode does not reduce the 100-unit bound. The byte bound keeps the hex
stream ID within 512 characters. Invalid creates answer 400 before registration. URL-encode the whole ID as one segment, including any slashes.
File paths are relative POSIX paths; encode each segment separately.

- `PUT /api/folders/:folderId/files/*path` sends raw bytes. Supply `x-file-op-id`,
  `x-file-author`, and `x-file-origin`, percent-encoding each value with
  `encodeURIComponent`. Invalid encoding answers 400. Supply `x-file-base` with the version last
  seen, or omit it for a file you have not seen. Text up to 64 KiB is inline;
  other files use immutable blobs. Files above 8 MiB answer 413.
- `DELETE` on that path uses the same headers. A new delete requires a live head;
  missing or already deleted files answer 404 without an append. Writes return
  `{ version, overwrote? }`.
  Replaying an opId for the same path returns its original version and overwrite
  mark, even after another writer changes the head. Reusing it for a different
  path in the same folder answers 400.
- `GET` returns raw bytes with `x-file-version` and a hash ETag. `?at=<version>`
  reads that exact entry for the path. `?minVersion=<version>` waits for the
  folder log to reach that offset, then reads the current file head. It answers
  503 if the log has not reached it after 10 seconds.
- `GET /api/folders/:folderId/history/*path` returns `{ version, operation }`
  entries, including tombstones and `overwrote` marks.
- `GET /api/blobs/:hash` returns immutable bytes with long cache headers.

Versions are Streamsy offsets, compared lexically. Directories are implicit.
Deletes keep history. A stale write wins in commit order and records the unseen
head in `overwrote`. There is no rename operation.

## Index and live reads

All index reads are eventually consistent with folder commits. They retain folder
log versions, so a search hit can be opened with `?at=<version>`.

- `GET /api/ls?folder=docs&prefix=guide/&glob=**/*.md` returns current live
  manifest rows. All filters are optional and combine. Prefix and glob apply to
  the path within each folder. Globs use picomatch with dotfiles enabled. URL-encode
  query values. Each row has `id`, `folderId`, `path`, `version`, `hash`, `size`,
  `updatedAt`, and `author`. The id is the JSON tuple `[folderId, path]`.
- `GET /api/search?q=durable&folder=docs` searches current UTF-8 text, including
  text stored as blobs. The optional folder filter is exact. Binary content and
  deleted files are removed. FTS5 query syntax is accepted; malformed queries
  answer 400 and an empty query returns no hits. Results contain folder, path,
  version, and snippet, sorted by FTS5 bm25 relevance with folder/path tie breaks,
  with a limit of 100. A snippet is an array of `{ text, match }` segments. Render
  every segment as text; `match` says which segments to highlight.
- `GET /streams/manifest?offset=-1` serves the read-only Durable State contract
  for the `file` collection. Consume every page, then tail from
  `stream-next-offset` with `live=long-poll`. Upserts replace the row and deletes
  remove it. The manifest stream contains retained change events, not just heads.
- `GET /streams/changes?offset=-1` reads the shared JSON change feed. Each entry
  carries folder, path, folder-log version, size, deletion status, optional hash
  and overwrite mark, author, and timestamp. The response's
  `stream-next-offset` is the sync cursor, distinct from the folder version.
  Read pages until `stream-up-to-date: true`, then use
  `?offset=<cursor>&live=long-poll`. A 204 response means no change before the
  long-poll timeout; continue with the returned cursor.

The storage-wide page limit stays at one item for exact entry versions. Clients
must follow pagination for both streams. New log entries carry a server timestamp;
older entries without one use the epoch timestamp during projection rebuilds.
The timestamps never choose the winning write.

On restart, the server restores bindings from the complete registry before tailing
new folders. Every projection resumes its saved offset. The rebuild test stops
followers, clears output streams and FTS rows, and replays a new generation from
the folder logs. A rebuild resets feed cursors and requires clients to bootstrap
again. Cross-folder replay can interleave differently, while each folder retains
its original commit order and final heads.

`/api/ls` also returns `stream-next-offset`: the shared changes offset consumed by
these manifest rows, read in the same SQL transaction. Bootstrap files from the
rows, then tail `/streams/changes` from this header. A lagging manifest is safe:
changes newer than its cursor are replayed. Filtered lists still return the global
cursor; keep the same folder selection when tailing.

## Sync CLI

With the server running, create folders through the API, then mirror them:

```sh
bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-docs" --folders docs,notes --write notes
```

`--folders` selects folders; omit it to discover every folder with files. `--write`
accepts a comma-separated list of writable folders and defaults to none. An edit
in a read-only folder is restored from the server and logged. Writable folders
must already exist on the server. Folder IDs and file names may contain Unicode.
The comma syntax cannot select an ID containing a comma; the exported runner
accepts arrays for that case.

Files appear at `<dir>/<folderId>/<path>`. A slash in a folder ID stays a nested
directory. `sync(options)` in `sync/engine.ts` accepts a `folderDirectory(id)`
function returning a relative directory, so another host can choose its layout.
Mappings must stay inside the mirror and use disjoint roots. Absolute IDs,
traversal-like IDs, overlapping roots such as `docs` and `docs/api`, and the
reserved `.folder-sync` directory require a different mapping or selection.
Symlinks, special files, and files over 8 MiB are skipped and logged once per
path and metadata change during each daemon run. Run one daemon per mirror directory.

The CLI bootstraps from `/api/ls` and its consumed cursor, fetches current heads,
and tails every change page using long-poll. It keeps folder versions separate
from the feed cursor and ignores older or duplicate versions. A cursor beyond
the server's tail, or a regressing response cursor, triggers bootstrap again.
The selection and server URL are bound to the state directory; use another
`--dir` when changing them. Restarting with the same options resumes the cursor
and any pending writes. `Ctrl+C` or `SIGTERM` closes watchers, requests, and SQLite;
the Effect runtime uses exit code 130 for interruption.

`.folder-sync/state.sqlite` stores known heads, observed versions and hashes, tombstone
versions, the feed cursor, and an immutable write outbox. A saved pull intent
whose observed version has not settled is finished after restart. The watcher ignores
`.folder-sync`, waits 100 ms for editor bursts to settle, and hashes the resulting
files. A periodic reconciliation scan also covers coalesced or missing watcher
events. Pulled bytes with a known hash do not push back. A temp-file-then-rename
save within the settle interval is observed as the final file. Other longer-lived
temporary files can be synced.

Local edits are queued before sending. Offline writes and retryable HTTP failures
(5xx, including `minVersion` 503, 409, and 429) retry with exponential backoff from
100 ms to a 3 s ceiling. Replays reuse the original opId, base, and bytes. Pending
edits stay on disk while remote changes arrive. After acceptance the CLI refreshes
the authoritative head, so a lost-response replay cannot restore stale content
over another writer. An unseen overwrite is logged. A delete pulled from the
server retains its tombstone version for a later local recreation's base.
401 and 403 stop the daemon with a clear authorization error and keep all pending
entries queued. Correct the token and restart. HTTP 408, 409, 429, transport errors,
and 5xx retry with backoff. Other non-retryable HTTP write errors move the entry and its bytes to the SQLite
`rejected` table, log the reason, and allow later writes to continue. Other local
I/O failures stop the CLI with an error; the
outbox remains on disk for inspection or a corrected restart.

This remains a demo: every scan hashes selected files, history and manifest reads
grow with retained events, and outbox bodies are stored in SQLite without a disk
quota. Folder logs and outbox entries retain the server's 8 MiB file limit.

On the first bootstrap, identical existing files adopt the server version without
writing it again. Different local bytes are preserved as
`name.conflict-<origin>[-n].ext` siblings while the server head keeps the original
path. In writable folders those siblings sync as new files. Existing files with
no server head become new writes.

Read-only edits and new local files are moved before restoration to
`<dir>/.folder-sync/rejected/<folderId>/<path>` (using the configured folder mapping).
An existing backup gets a numeric suffix before its extension (`read-1.md`); no earlier backup is replaced. The CLI
logs the full backup path. These saved bytes stay outside the watched mirror.

The default ignore list covers Vim `.swp` and `.swx` files, `~` backups and `4913`
probes; Emacs `#file#` and `.#file` files; and `.DS_Store`. Matching names at any
path depth are ignored by scans, bootstrap, live pulls, and watcher triggers.

Bootstrap, per-change head reads, and the post-push head read run inside the local
sync gate. A slow server or a `minVersion` timeout can delay scans and other
mirror decisions for up to the server's ten-second wait.

Both hosts return a persisted `stream-epoch` on `/api/ls` and all changes responses,
including HEAD and long-poll. The CLI checks the epoch before tailing and on every
page. A changed epoch triggers a fresh bootstrap even when the feed cursor still
fits. Stored folder versions and unsent bases are cleared; pending bytes and opIds
stay queued. A pending write in a replacement database uses an absent base and may
carry an overwrite mark. Rebuild tools must rotate the epoch with
`rotateFeedEpoch` in the same owner transaction that resets outputs. There is no
public rebuild endpoint. A live read already parked during a reset can take the
long-poll timeout to observe it; restart checks immediately.

Atomic replacement still has a final local read-to-rename window in which an
external editor save can be replaced. SQLite and disk replacement have no shared
transaction or explicit filesystem fsync.

## Rejected writes and operational limits

Inspect rejected writes without connecting to the server or running a watcher:

```sh
bun run --cwd examples/folder-sync sync rejected --dir "$PWD/examples/folder-sync/mirror-a"
```

The command opens existing SQLite state read-only and prints a JSON array of
sequence, folder, path, operation ID, base, byte size, HTTP status, and reason.
It does not print file bodies or requeue writes. Read-only backup files are
separate from rejected HTTP operations and remain under `.folder-sync/rejected`.

Index followers log terminal failures and restart after one second, resuming
persisted checkpoints. A permanent failure repeats that log until repaired;
HTTP head writes can continue while an index catches up.

The Bun listener remains open for a trusted local demo. The Worker requires a
writer token for every write and keeps reads open. There is no deployed instance
or phone cache. One-item stream
pages and retained manifest events make browser load time grow with history.
Head checkpoints keep all paths and operation receipts; history reads and search
preparation scan folder logs. Browser bundle size and sync scans suit a small
demo corpus. Rejected records, backups, and blob storage have no retention quota.
Browser drafts are held in memory; navigating to another file or reloading loses
an unsaved draft. Search shows at most 100 hits. The CLI bootstraps automatically after a feed epoch changes.

## Local Cloudflare Worker

Run from the repository root:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync dev:worker
```

This command builds the UI and Worker, then starts Miniflare/workerd with local
SQLite Durable Objects, local R2, and Workers static assets. It creates no remote
resources and needs no Cloudflare account or login. `PORT` defaults to 1339.
`FOLDER_SYNC_WORKER_STATE` selects the persistent local state directory; the
default is `examples/folder-sync/.scratch/worker`. Stop with Ctrl-C.

Use `Authorization: Bearer local-demo-token` on folder creation, PUT, DELETE,
and administrative writes. The check runs before the object router. A missing
or wrong token answers 401. Reads remain open. For the seed:

```sh
FOLDER_SYNC_WRITER_TOKEN=local-demo-token bun run --cwd examples/folder-sync seed
```

For a writable mirror:

```sh
bun run --cwd examples/folder-sync sync --server http://localhost:1339 --dir "$PWD/examples/folder-sync/mirror-a" --write docs/root --token local-demo-token
```

Set the browser's **Writer token** field before saving. It stores the token in
localStorage for that origin and supplies it on saves. Clear the field to remove
it. Unsaved drafts still live only in memory.

The `alchemy.run.ts` stack declares two SQLite Durable Object classes, an R2
bucket, static assets, a redacted writer token, and a cron every minute. This
batch uses the local workerd runner; the stack is typechecked. No Alchemy apply,
Cloudflare provisioning, or deployment has been run.

Each opaque folder ID routes through `Placement.byRoute` to one folder object.
Placement decodes the internal hex stream segment, so the object name uses the
original ID rather than its doubled hex form. An index object owns the registry,
notice inbox, change feed, manifest, and FTS5 table.

A folder write and its durable notice enqueue commit in one SQL transaction.
Notices carry the folder version, overwrite mark, and inline text or R2 hash.
`folderId/version` deduplicates intake; search needs no remote folder-log read.
Pending notices drain before and after each folder request with a two-second
bound per pass. The oldest pending notice blocks later ones until it succeeds,
so retry backoff cannot reorder folder commits.

A failed drain keeps the notice queued and the write still returns its receipt.
The Worker scheduled handler lists registered folders and asks each to drain.
Without traffic, retry latency is bounded by the one-minute cron interval, plus
processing time. Larger backlogs may take multiple passes. The local runner
calls the same pass every minute because Miniflare does not fire cron itself.
`POST /api/admin/drain`, protected by the writer token, triggers it immediately.

Index requests catch up notices, changes, manifest, and search before answering.
Projection failures are logged and answer 503 with `Retry-After: 1`; later
requests and cron passes retry from checkpoints. Both hosts read manifest rows,
the consumed change cursor, and epoch in one SQL transaction.

The stock Streamsy expiry alarm remains untouched. The package follow-up is an
application alarm hook in `@streamsy/serve`; the stock host currently runs only
stream expiry in its alarm. This example uses request and cron delivery instead
of application alarms or long-lived projection followers in objects.

Run the local checks:

```sh
bun run --cwd examples/folder-sync smoke:worker
bun run --cwd examples/folder-sync test:worker
```

`smoke:worker` runs the shared HTTP smoke against a fresh local Worker.
`test:worker` verifies scheduled recovery, auth, SQLite query behavior, R2 and
object persistence, then runs the entire sync integration suite against workerd.
The ordinary `test` command keeps all Bun checks and includes the Worker host
checks. Set `FOLDER_SYNC_SCRATCH` to place temporary test data in a chosen directory.

Worker change entries include optional inline content or a blob hash. All reads
are public, including these feed fields. Every stream still uses one-item pages.
Delivered outbox rows and receiver dedup rows are retained without a quota.
There is no public index-rebuild command; reset procedures must rotate the epoch
and clear derived outputs, checkpoints, and index head tables together.
