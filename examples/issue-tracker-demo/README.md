# Issue tracker demo

A complete Streamsy application with an Effect backend. An `HttpApi` handler
reads the workspace stream from the beginning, folds it into Durable State,
validates the mutation, and appends one conditional change. The browser replays
that same stream into TanStack DB with `@durable-streams/state/db`.

![Architecture diagram: browser actions post to an HttpApi handler, IssueTracker.mutate reads, folds and appends to the workspace stream, Serve.mount serves the stream read-only, and createStreamDB follows it in the browser.](../../site/assets/docs/demos/issue-tracker-demo-architecture.svg)

The [guide](https://streamsy.dev/docs/demos/issue-tracker) explains each
numbered step with excerpts from this folder.

## Run it

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/issue-tracker-demo dev
```

Open <http://localhost:1338>. The seeded `main` workspace is ready immediately;
the share control creates isolated workspaces with their own stream.

| Variable           | Default       | Meaning                                               |
| ------------------ | ------------- | ----------------------------------------------------- |
| `PORT`             | `1338`        | HTTP port                                             |
| `ISSUE_TRACKER_DB` | unset         | SQLite file; unset keeps a process-local store        |
| `NODE_ENV`         | `development` | `production` turns off hot reload and console echoing |

## The product

This app is the base for a family of variants. Each variant changes one side,
the server or the browser, and keeps this product and its wire contract.

- A seeded `main` workspace has two projects, five issues and one comment.
  "Streamsy Demo" has two of its three in-progress places taken.
- Create projects and issues. Rename an issue inline. Set its status. Comment.
- Filter the issue list by status. Each filter shows its count.
- Set a display name. It shows on your comments and as the last writer of an
  issue. The browser keeps it in `localStorage`.
- A project can have at most three issues in progress. The server refuses a
  fourth with HTTP 422 and the `WipLimitReached` tag.
- A local change shows at once, faded, and turns solid when the stream confirms
  it. A refused change goes away, and a short message says why.
- An "Offline" badge shows while the page cannot reach the stream.
- The server stamps every time field. The API ignores times in a body.

### Try it by hand

Every variant must pass this script.

1. Open `main`. See two projects, five issues and one comment.
2. Set a display name. Add a comment. The comment shows the name.
3. Create a project. It becomes selected. Add an issue. It shows at once,
   faded, then solid.
4. Rename the issue. Change its status. Filter by status. The counts change.
5. Open the same URL in a second tab. Each change in one tab shows in the other.
6. In "Streamsy Demo", move a different issue to "In progress" in each tab at
   the same time. One change stays. The other rolls back with a message.
7. Create a shared workspace. Open its link in a new tab. It shows only its
   starter project.
8. With `ISSUE_TRACKER_DB` set, restart the server. All data is still there.

## Persistence

Set `ISSUE_TRACKER_DB` when state should survive a server restart:

```bash
ISSUE_TRACKER_DB=.data/issues.sqlite bun run --cwd examples/issue-tracker-demo dev
```

Create the parent directory before starting the server.

## How it works

`server/index.ts` is one layer graph, launched with
`BunRuntime.runMain(Layer.launch(...), { teardown: Serve.teardown })`. `Config` reads `PORT`,
`ISSUE_TRACKER_DB` and `NODE_ENV`. The graph opens storage, builds the
`IssueTracker` service, seeds the `main` workspace, and then starts the
listener. `BunHttpServer.layer` keeps Bun's HTML bundle and hot reload for
`/*`; `/api` and `/streams` fall through to one Effect `HttpRouter`. That
router holds two things: `HttpApiBuilder.layer(IssueTrackerApi)` for the JSON
API, and `Serve.mount(Workspaces, "/streams/workspace/:workspaceId")` from
`@streamsy/serve` for the read-only Durable State route. `Serve.layer` serves
that router. On SIGINT or SIGTERM it answers parked long polls with 204, the
listener stops, storage closes last, and the process exits 0.

The wire contract lives in the private workspace package
[`@streamsy/issue-tracker-demo-contract`](../issue-tracker-demo-contract), so
the variants import the same schemas. Its `api.ts` declares `IssueTrackerApi`:
one group each for workspaces, projects, issues and comments. Effect schemas in
its `api-schema.ts` check the workspace id, the partial entity body and its
optional `txid`, and describe the success bodies and the `{ "error": string }`
error bodies. The same entity schemas in its `state-schema.ts` type the stream
collections on the server and, through `Schema.toStandardSchemaV1`, the
StreamDB collections in the browser.

`IssueTracker.mutate` is the read, decide, append recipe in one
`Workspaces.decide` call: fold the workspace's State changes, validate against
the resulting view, and append with `expectedOffset`. `decide` retries on an
`OffsetMismatch` only, so parallel writers cannot silently overwrite one
another; after three retries the API answers 409. The validation rules in `server/state.ts` return
the change to append or a typed error, and the handlers map those errors to
400, 404, 409 or 422. The work-in-progress rule reads every issue of the
project, so it holds only because each attempt sees the state at the head it
appends to: when two writers race for the last place, the loser's retry sees
the winner's issue and fails with 422. Project, issue, and comment changes share one
`Streamsy.state` declaration, `Workspaces`, whose collections type both reads
and writes. `DurableState.changes` stamps each change for the append. Because
the tracker appends its own events rather than projecting an upstream, each
change records the workspace head that `decide` read as its source offset.

The server keeps no workspace cache. In the browser, `createStreamDB` follows
the public workspace stream and exposes projects, issues, and comments as
TanStack DB collections, including optimistic writes keyed by the transaction
id returned from the API. `src/queries.ts` derives every view from those
collections with live queries: the filtered and sorted issue list, the counts by
status and by project, and the comments of each issue. Each result row keeps
TanStack DB's `$synced` flag, which marks a pending local change.

## Verify

```bash
bun run --cwd examples/issue-tracker-demo test:unit
bun run --cwd examples/issue-tracker-demo typecheck
bun run --cwd examples/issue-tracker-demo smoke:http
```

The offline HTTP smoke calls the API through an `HttpApiClient` built from
`IssueTrackerApi`. It covers the seed, projects, issues, comments, workspace isolation,
two concurrent requests that race for the last in-progress place (one 200, one 422),
ten concurrent writers landing with the exact event count and no duplicates,
raw probes for bodies the typed client cannot send, raw stream writes (405), a
shutdown that drains a parked read with 204, and a SQLite process restart. The in-process
recipe tests use a read barrier to force a CAS conflict and prove that the
losing writer retries against the new state, the same race against the
work-in-progress rule, and a writer that loses every race to prove the 409 after
three retries.

The smoke takes an optional app folder, for example
`bun run scripts/http-smoke.ts ../issue-tracker-demo-event-sourced`. It starts
that folder's `server/index.ts` and runs the same checks, so every server
variant is held to this wire contract. Each read after a write waits for the
write's txid on the stream, because a projected stream can trail the answer.

## Limits

With no database path, restarting the server starts with a fresh memory store.
Every mutation replays one complete workspace, so the recipe suits small
workspaces, not a large-workspace read model. Shared workspace ids are
unguessable-ish links, not an authorization boundary.

Guide: [streamsy.dev/docs/demos/issue-tracker](https://streamsy.dev/docs/demos/issue-tracker).
