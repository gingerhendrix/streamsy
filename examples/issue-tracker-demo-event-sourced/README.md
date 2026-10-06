# Issue tracker demo, event sourced

The [issue tracker demo](../issue-tracker-demo) with an event-sourced server.
The product, the browser code and the wire contract
([`@streamsy/issue-tracker-demo-contract`](../issue-tracker-demo-contract)) do
not change. A command appends domain events to an event stream. A projection
writes the same project, issue and comment rows to the same State stream at
`/streams/workspace/:workspaceId`.

The [guide](https://streamsy.dev/docs/demos/issue-tracker-event-sourced) shows
only what differs from the base app.

## Run it

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/issue-tracker-demo-event-sourced dev
```

Open <http://localhost:1338>. The settings are the base app's settings.

| Variable           | Default       | Meaning                                                          |
| ------------------ | ------------- | ---------------------------------------------------------------- |
| `PORT`             | `1338`        | HTTP port                                                        |
| `ISSUE_TRACKER_DB` | unset         | SQLite file for events, rows and checkpoints; unset keeps memory |
| `NODE_ENV`         | `development` | `production` turns off hot reload and console echoing            |

## How it differs

| File                      | Role                                                                                                        |
| ------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `server/events.ts`        | The domain events: `ProjectCreated`, `IssueCreated`, `IssueRenamed`, `IssueStatusChanged`, `CommentAdded`   |
| `server/streams.ts`       | The event stream, the public State stream, the workspace registry, and storage with `CheckpointStore.layer` |
| `server/command-view.ts`  | A fold projection that holds what the rules read                                                            |
| `server/decide.ts`        | The base app's rules, as decisions that return events                                                       |
| `server/issue-tracker.ts` | `Projection.decide` for each command, workspace creation, and boot                                          |
| `server/read-model.ts`    | A `Commit.producer` projection that writes the rows with each event's txid and time                         |
| `server/watchers.ts`      | One read model runner for each workspace, at boot and on create                                             |
| `server/index.ts`         | The base app's layer graph, and the base app's `public/index.html`                                          |

The routes in `server/routes/` and `server/api.ts` are the base app's, with
the variant's domain imports. One status differs: a patch with no title and no
status answers 400 "Nothing to change", because a write must append an event
for the browser's txid.

`awaitOffset` in a success body is the event stream offset after the command's
events. The browser ignores it and waits on the txid, as in the base app.

### Try it by hand

This is the base app's script. Every variant must pass it.

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
8. With `ISSUE_TRACKER_DB` set, restart the server. All data is still there,
   and new changes still show.

## Verify

```bash
bun run --cwd examples/issue-tracker-demo-event-sourced test:unit
bun run --cwd examples/issue-tracker-demo-event-sourced typecheck
bun run --cwd examples/issue-tracker-demo-event-sourced smoke:http
```

The unit tests cover the event fold, each decision and the work-in-progress
race through `Projection.decide`, the rows and headers that the read model
writes, a restart over SQLite that resumes a shared workspace's runner, and the
API. The HTTP smoke is the base app's smoke, run against this folder. It waits
for each txid on the State stream before it reads the rows.

## Limits

The read model trails the event append. The txid wait hides this from the
writer. Each projection saves its whole state at each unit, which suits a demo
workspace. The event stream has no route yet.
