# Hacker News newest stream demo

A Bun server polls Hacker News and writes one Durable State stream,
`hackernews/newest`. A checkpointed fold, `newestRows`, folds the stream into
the current story rows. Before each append, the poller runs the projection,
reads the rows and the offset they are at, diffs them against the fetch, and
appends the changes with `expectedOffset: offsets.input`. `Serve.mount`
publishes the stream at `/state/newest`. The React browser replays from `-1`
into TanStack DB through `createStreamDB`, then follows live changes.

The poller is the working example of the "Read, decide, append" recipe in the
[projections docs](https://streamsy.dev/docs/projections/state#read-decide-append).

```text
HN API --fetch--> poller --diff--> append(expectedOffset: offsets.input) --> hackernews/newest
                  ^                                                           |     |
                  | Projection.read: state + offsets                          |     +--> Serve.mount /state/newest --> browser
                  +---- Projection.run(newestRows) <-- fold -------------------+
```

The collection is `hn-story`, keyed by `id`. The projection declares its state
on the declaration and gives a fold step:

```ts
export const newestRows = Projection.fold({
  id: "hn-newest-rows",
  params: {},
  inputs: { input: storiesAddress },
  state: { schema: StoryRows, initial: {} },
  fold: (rows, { item }) => applyChange(rows, item),
}).at({});
```

SQLite keeps the stream, the projection checkpoint and the state row in one
file. The state row and the checkpoint commit in one transaction. About 50 rows
live in one state value because this set is small and bounded; the projections
docs advise application tables for large row sets.

One pass of the recipe is `reconcileNewest` in `src/server/poller/reconcile.ts`:

```ts
export const reconcileNewest = Effect.fn("NewestStoriesPoller.reconcile")(function* (
  fetched: FetchResult,
) {
  // Run to the tail. A run with a limit would leave the offset behind the tail.
  yield* Projection.run(newestRows);
  // The rows and the offset they are at, from one transaction.
  const { state: rows, offsets } = yield* Projection.read(newestRows);
  const changes = diffNewest(rows, fetched);
  // An append needs items. No change appends nothing, so it does not check the tail.
  if (!Array.isReadonlyArrayNonEmpty(changes)) return { rows, changes, offset: offsets.input };
  const events = DurableState.changes(storiesAddress, { offset: offsets.input }, changes);
  const stories = yield* HackerNewsStories;
  const appended = yield* stories.append({}, events, { expectedOffset: offsets.input });
```

Each pass fetches the newest ids and all their stories once. Unchanged stories
produce no writes. Departed ids produce deletes, including ids that left during
server downtime. An `OffsetMismatch` triggers up to three retries of the whole
pass. Each retry runs the projection, reads again and rebuilds the diff from
durable rows without refetching HN. After the retry limit, status records the
error and the next poll tries again. There is no background projection watcher
or in-memory story cache.

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
`HttpRouter.serve` hosts one `Layer.mergeAll` app on the Streamsy Bun
`listener`. `BunRuntime.runMain` owns the process. On SIGINT or SIGTERM,
`drainOnShutdown` answers live reads before the router closes.

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

```bash
curl -i 'http://localhost:1339/state/newest?offset=-1'
curl -s http://localhost:1339/api/status
curl -s -X POST http://localhost:1339/api/poll
```

`GET /api/status` reports polling, last start/completion times, last error,
last successful story count, last upserts/deletes, total conflicts retried and
last accepted/appended offset, plus the stream URL and poll settings. Counters
are process-local. `POST /api/poll` runs one pass; concurrent calls join the
in-flight pass, including a scheduled poll. Completion is reported even on
failure; check `lastPollError` for the result.

## Where to look

- `src/server/stories.ts`: the fixed `Streamsy.state` declaration and change types.
- `src/server/poller/poller.ts`: fetch, retry on `OffsetMismatch`, status counters and coalescing gate.
- `src/server/hnews.ts`: the `HackerNewsApi` layer over the Effect `HttpClient`.
- `src/server/newest-rows.ts`: `Projection.fold` with `state:` and `fold:`, and pure `applyChange`.
- `src/server/poller/reconcile.ts`: `reconcileNewest` (run, read, decide, append) and pure `diffNewest`, with stable ordering and unchanged-write suppression.
- `src/server/http.ts`: State, status, poll and static routes.
- `src/server/streams.ts`: shared SQLite stream/projection host, memory option and boot creation.
- `src/server/index.ts`: `runMain`, the listener, the drain and the application layers.
- `src/server/config.ts`: host and HN poll settings as one `Config`.
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
at 50 rows. Recovery means closing
the old collection and opening a fresh one from `-1`. After a server reset,
reload the page: it does not automatically reopen its collection.

The installed `@durable-streams/state` binding updates an existing key in place
without doubling rows, but merges its fields. A field omitted by a later upsert
keeps its old value in the browser, both live and on replay.

Individual failed or skipped HN item fetches keep an existing row while its id
is still newest. A later successful fetch can update it; departure from the
newest id set deletes it. HN's id list and item responses are separate requests,
so a poll is not an atomic HN snapshot.

`Projection.read` gives the rows and their offset from one transaction. If
another process appends after the read, `expectedOffset` refuses the stale
append and the poller retries. A no-change pass appends nothing and does not
check the tail; later polls reconcile later writes. `expectedOffset` prevents
stale writes, but does not order the HN fetches of different processes by
freshness. Steady contention can use up the three retries. Projection
checkpoint conflicts and other operational errors are recorded for the next
scheduled pass, not retried as append conflicts.

`HN_DB=memory` starts fresh on every process restart.

Guide: [streamsy.dev/docs/demos/hackernews-newest-stream](https://streamsy.dev/docs/demos/hackernews-newest-stream).
