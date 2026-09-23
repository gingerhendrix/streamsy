# Issue tracker projection

This SQLite example combines five workspace inputs in two checkpointed projections.
Commands append canonical issue facts through `transact`, run both workspace members
under their keyed mutexes, and answer from SQL. Same-owner change feeds also run both
members. Startup attempts to catch up each configured member before accepting requests.
Startup and watcher failures are logged with the projection key and do not stop other
members or the server. A failed member can serve stale rows or a 503 document until
repaired; a terminated watcher does not automatically restart.

```bash
bun run --cwd examples/issue-tracker start
bun run smoke:issue-tracker
bun run app:issue-tracker
bun run smoke:issue-tracker-serve
ISSUE_TRACKER_DB=tracker.sqlite bun run --cwd examples/issue-tracker run:once acme
```

`PORT` defaults to `1340`. `ISSUE_TRACKER_DB` selects the SQLite file (default:
`/tmp/streamsy-issue-tracker-<pid>.sqlite`). `ISSUE_TRACKER_WORKSPACES` is the
comma-separated fixed member list and defaults to `acme,live`.

The server composes protocol, command and declared output routes with `Layer.mergeAll`
and runs `HttpRouter.serve` with a scoped Bun listener. The routes are:

| Route                                                       | Result                                            |
| ----------------------------------------------------------- | ------------------------------------------------- |
| `/streams/*`                                                | Durable Streams protocol                          |
| `POST /api/workspaces/:workspaceId/commands`                | Create, status and assign commands                |
| `/api/workspaces/:workspaceId/status`                       | SQL projection checkpoint offsets                 |
| `/api/workspaces/:workspaceId/drafts`                       | SQL notification drafts                           |
| `/state/workspaces/:workspaceId/issues?offset=-1`           | `board` Durable State changes, keyed by `issueId` |
| `/state/workspaces/:workspaceId/label-counts?offset=-1`     | `labelCounts` changes, keyed by `labelId`         |
| `/feed/workspaces/:workspaceId/issue-transitions?offset=-1` | Accepted issue facts, retaining their event ids   |
| `/document/workspaces/:workspaceId/summary`                 | Compact counts, with ETag and conditional 304     |

The old `/api/.../issues` and `/api/.../changes` reads are replaced by the board and
transition routes. State readers start at `-1`, apply upserts/deletes, and follow
`stream-next-offset` until `stream-up-to-date`. There is no snapshot; retained history
and replay cost grow. Reads do not run projections: missing output streams return 404
and missing summary values return 503, including workspaces outside the runner list.
The raw protocol routes are open in this local demo; there is no authentication.

The application owns `issues`, `issue_labels`, `projects`, `users`, `labels`,
`issue_changes`, and `notification_drafts`. The fused `issueRows` projection writes
those tables and its checkpoint through the same SQL transaction. The separate
`tracker` named family constructs `Projection.outputs` members: `board` and
`labelCounts` use `Output.rows`, `transitions` uses `Output.stream`, and `workspace`
uses `Output.value`. Each stream has its own workspace-specific identity and sequence.
`@streamsy/projection` owns only its `streamsy_projection_v1_*` tables, including the
persisted value. The command path still reads facts rather than Projection.State.

`ProjectBoardCard` extends the issue row with label ids and nullable project/assignee
names. Label counts count attached memberships of existing issues for each catalog
label, including zero counts; deleting a catalog label emits `Output.remove(labelId)`
without `old_value`. Membership ids remain on cards until a detach fact arrives.
`IssueTransition` uses the existing `IssueEvent` schema, excluding stale facts rejected
by the sequence fold. The `workspace` value uses `WorkspaceState` to retain source rows
and membership tombstones for deterministic replay. The full fold state stays private
to the server. A `Serve.ValueSource` wrapper serves only `WorkspaceSummary`:
`workspaceId`, `issueCount`, `doneCount`, `labelCount`, and `projectCount`. Counts of
labels and projects are catalog row counts. Count-neutral changes preserve its ETag.
The state row still grows with workspace size and is rewritten per unit and decoded
per document read.

Both projections apply catalog deletes, including key-only deletes, in the member's
workspace. SQL catalog upserts use the member workspace too. A catalog value naming
a different workspace fails with `CatalogWorkspaceMismatch`, naming both workspaces;
SQL writes in that unit roll back and checkpoints do not advance. Such malformed
input requires operator repair/rebuild; valid key-only deletes do not stop progress.

The named fold uses only the pinned batch and prior value, never mutable SQL rows.
SQL and output checkpoints are independent, and there is no atomicity across outputs
or across the two projections. A command can be accepted before projection failure;
this demo does not add command idempotency. The new `smoke:issue-tracker-serve` script
(package-local `smoke:serve`) checks rows, a key-only delete, transitions, document
ETags/304, restart on the same SQLite file, and another command after restart. Client bindings remain a later slice.
