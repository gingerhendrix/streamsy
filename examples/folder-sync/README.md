# Folder sync

An append-only file log per folder, with commit-order heads and retained history.
The Bun server exposes the file API, a shared change feed, a live manifest, and FTS5 search.
Later batches add local sync and the browser.

```sh
bun run --cwd examples/folder-sync start
FOLDER_SYNC_DB=./folder-sync.sqlite bun run --cwd examples/folder-sync start
bun run --cwd examples/folder-sync test
bun run --cwd examples/folder-sync smoke:http
```

The default port is 1339 (`PORT` overrides it). An unset or empty `FOLDER_SYNC_DB`
uses transient SQLite (`:memory:`), including FTS5. Setting a path persists logs,
checkpoints, blobs, and search in the named SQLite file. The folder service also
has a MemoryStorage test host; the indexed server requires SQL transactions.
Tests use a scoped directory under `.scratch/`; `FOLDER_SYNC_SCRATCH` overrides it.

Create a folder with `POST /api/folders` and JSON `{ "folderId": "docs", "title": "Docs" }`.
Folder IDs are opaque strings. URL-encode the whole ID as one segment, including any slashes.
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
  the path within each folder. Globs use Bun's POSIX glob matching. URL-encode
  query values. Each row has `id`, `folderId`, `path`, `version`, `hash`, `size`,
  `updatedAt`, and `author`. The id is the JSON tuple `[folderId, path]`.
- `GET /api/search?q=durable&folder=docs` searches current UTF-8 text, including
  text stored as blobs. The optional folder filter is exact. Binary content and
  deleted files are removed. FTS5 query syntax is accepted; malformed queries
  answer 400 and an empty query returns no hits. Results contain folder, path,
  version, and snippet, sorted by folder and path, with a limit of 100. Snippets
  contain `<mark>` delimiters; treat file content as text when displaying them.
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
