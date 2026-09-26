# Hacker News newest stream demo

A Bun server polls Hacker News and writes one Durable State stream,
`hackernews/newest`. Before each append, an on-demand projection folds the stream
into current rows. The poller diffs those rows against the fetch, then appends
changes with `expectedOffset` set to the projection's accepted input offset.
`Serve.state` publishes the stream at `/state/newest`. The React browser replays
from `-1` into TanStack DB through `createStreamDB`, then follows live changes.

```text
HN API --fetch--> poller --diff--> append(expectedOffset = token) --> hackernews/newest
                  ^                                                       |     |
                  | rows + token                                          |     +--> Serve.state /state/newest --> browser
                  +---- Projection.run(newestRows) <-- fold ---------------+
```

The collection is `hn-story`, keyed by `id`. SQLite keeps the stream, projection
checkpoint and fold state in one file. The fold state and checkpoint commit in
one transaction. About 50 rows live in fold state because this set is small and
bounded; the projections docs advise application tables for large row sets.

Each pass fetches the newest ids and all their stories once. Unchanged stories
produce no writes. Departed ids produce deletes, including ids that left during
server downtime. An `OffsetMismatch` triggers up to three retries, each running
the projection and rebuilding the diff from durable rows without refetching HN.
After the retry limit, status records the error and the next poll tries again.
There is no background projection watcher or in-memory story cache.

## Run it

From the repository root:

```bash
bun install
bun run build
bun run --cwd examples/hackernews-newest-stream dev
```

Open <http://localhost:1339>. `dev` builds the browser; `dev:api` runs only the
server and can serve an existing browser build. The page, status API, poll API,
and State route share the same origin, so CORS middleware is unnecessary.
`HttpRouter.serve` hosts one `Layer.mergeAll` app with a scoped Bun listener.

| Variable              | Default                                 | Meaning                  |
| --------------------- | --------------------------------------- | ------------------------ |
| `PORT`                | `1339`                                  | HTTP port                |
| `HN_API_BASE`         | `https://hacker-news.firebaseio.com/v0` | Hacker News API          |
| `HN_POLL_INTERVAL_MS` | `60000`                                 | Delay between polls      |
| `HN_NEWEST_LIMIT`     | `50`                                    | Size of the newest set   |
| `HN_DB`               | `.data/hackernews.sqlite`               | SQLite file, or `memory` |

The stream id is fixed. The app creates it at boot, so the first
`GET /state/newest?offset=-1` returns 200 with an empty array even before the
first poll. Resume with the returned `stream-next-offset`; `live=long-poll`
waits for more. The route is read-only and returns State version and contract
headers. The browser sends State version 1. There are no raw `/streams` routes.

`GET /api/status` reports polling, last start/completion times, last error,
last successful story count, last upserts/deletes, total conflicts retried and
last accepted/appended offset, plus the stream URL and poll settings. Counters
are process-local. `POST /api/poll` runs one pass; concurrent calls join the
in-flight pass, including a scheduled poll. Completion is reported even on
failure; check `lastPollError` for the result.

## Where to look

- `src/server/stories.ts`: the fixed `Streamsy.state` declaration and change types.
- `src/server/poller/poller.ts`: fetch, projection run, CAS retry loop and coalescing gate.
- `src/server/newest-rows.ts`: `Projection.atomic`, `Projection.fold` and pure `applyChange`.
- `src/server/poller/reconcile.ts`: pure `diffNewest`, stable ordering and unchanged-write suppression.
- `src/server/http.ts`: State, status, poll and static routes.
- `src/server/streams.ts`: shared SQLite stream/projection host, memory option and boot creation.
- `src/server/index.ts`: scoped listener, poll schedule and shared application layers.
- `src/server/config.ts`: host and HN poll configuration.
- `src/state-schema.ts`: story, browser collection and status schemas.
- `src/client/db.ts`: browser collection factory also used by the HTTP smoke.

## Verify

```bash
bun run --cwd examples/hackernews-newest-stream typecheck
bun run --cwd examples/hackernews-newest-stream test
bun run smoke:hackernews
```

Tests cover the diff, fold, empty boot read, coalescing, unchanged polls, restart
reconciliation, competing appends and retry exhaustion. The offline smoke opens
an actual TanStack DB session from `-1`, changes a local HN fixture, and verifies
live upserts and deletes. Another session replays that history. It stops the
server, removes a story from the newest fixture, and restarts on the same SQLite
file. The first pass deletes the departed story without redundant upserts; both
a new session and the existing live session converge. Unchanged polls append
nothing.

## Limits

There is no snapshot. Every new browser replays the full retained history, which
must remain available and grows as stories change even when the newest set stays
at 50 rows. A server-side snapshot remains a follow-up. Recovery means closing
the old collection and opening a fresh one from `-1`. After a server reset,
reload the page: it does not automatically reopen its collection.

The installed `@durable-streams/state` binding updates an existing key in place
without doubling rows, but merges its fields. A field omitted by a later upsert
keeps its old value in the browser, both live and on replay.

Individual failed or skipped HN item fetches keep an existing row while its id
is still newest. A later successful fetch can update it; departure from the
newest id set deletes it. HN's id list and item responses are separate requests,
so a poll is not an atomic HN snapshot.

The projection run and fold-state load are separate reads. If another process
advances the stream between them, CAS rejects a stale append and the poller
retries. A no-change pass appends nothing and does not validate the tail;
subsequent polls reconcile later writes. CAS prevents stale writes, but does
not order different processes' HN fetches by freshness. Persistent contention
can exhaust the three retries. Projection checkpoint conflicts and other
operational errors are recorded for the next scheduled pass, not retried as
append conflicts.

An old `.data/hackernews.sqlite` holds the two old streams; this demo starts
clean under the new stream id `hackernews/newest`. There is no migration or
cleanup of old streams. `HN_DB=memory` starts fresh on every process restart.

Guide: [streamsy.dev/docs/demos/hackernews-newest-stream](https://streamsy.dev/docs/demos/hackernews-newest-stream).
