# Folder sync

An append-only file log per folder, with commit-order heads and retained history.
Batch A provides the Bun server. Later batches add indexing, local sync, and the browser.

```sh
bun run --cwd examples/folder-sync start
FOLDER_SYNC_DB=./folder-sync.sqlite bun run --cwd examples/folder-sync start
bun run --cwd examples/folder-sync test
bun run --cwd examples/folder-sync smoke:http
```

The default port is 1339 (`PORT` overrides it). An unset or empty `FOLDER_SYNC_DB`
uses memory. SQLite persists logs, checkpoints, and blobs in the named file.
Tests use a scoped directory under `.scratch/`; `FOLDER_SYNC_SCRATCH` overrides it.

Create a folder with `POST /api/folders` and JSON `{ "folderId": "docs", "title": "Docs" }`.
Folder IDs are opaque strings. URL-encode the whole ID as one segment, including any slashes.
File paths are relative POSIX paths; encode each segment separately.

- `PUT /api/folders/:folderId/files/*path` sends raw bytes. Supply `x-file-op-id`,
  `x-file-author`, and `x-file-origin`, percent-encoding each value with
  `encodeURIComponent`. Invalid encoding answers 400. Supply `x-file-base` with the version last
  seen, or omit it for a file you have not seen. Text up to 64 KiB is inline;
  other files use immutable blobs. Files above 8 MiB answer 413.
- `DELETE` on that path uses the same headers. Writes return `{ version, overwrote? }`.
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
