# Issue tracker projection

This SQLite example combines five workspace inputs in two checkpointed projections.
Commands append canonical issue facts through `transact`, run both workspace members
under their keyed mutexes, and answer from SQL. Same-owner change feeds also run both
members. Startup catches up configured workspaces before accepting requests.

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
| `/document/workspaces/:workspaceId/summary`                 | Workspace value, with ETag and conditional 304    |

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
`labelCounts` use `Output.rows`, `transitions` uses `Output.stream`, and `summary`
uses `Output.value`. Each stream has its own workspace-specific identity and sequence.
`@streamsy/projection` owns only its `streamsy_projection_v1_*` tables, including the
persisted value. The command path still reads facts rather than Projection.State.

`ProjectBoardCard` extends the issue row with label ids and nullable project/assignee
names. Label counts count attached memberships of existing issues for each catalog
label, including zero counts; deleting a catalog label emits `Output.remove(labelId)`
without `old_value`. Membership ids remain on cards until a detach fact arrives.
`IssueTransition` uses the existing `IssueEvent` schema, excluding stale facts rejected
by the sequence fold. `WorkspaceSummary` contains totals plus issue, membership and
catalog rows required for deterministic replay. Its membership tombstones preserve
sequence checks. The whole value is public in this demo and grows with workspace size.

Both projections reject a catalog input delete missing `old_value` with a clear
`ProjectionFault`, preserving their checkpoints; the SQL unit rolls back. Supplied
catalog producers include the old row. A bad input requires operator repair/rebuild.
Output readers accept key-only deletes normally.

The named fold uses only the pinned batch and prior value, never mutable SQL rows.
SQL and output checkpoints are independent, and there is no atomicity across outputs
or across the two projections. A command can be accepted before projection failure;
this demo does not add command idempotency. The new `smoke:issue-tracker-serve` script
(package-local `smoke:serve`) checks rows, a key-only delete, transitions, document
ETags/304, and restart on the same SQLite file. Client bindings remain a later slice.
