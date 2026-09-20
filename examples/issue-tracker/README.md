# Issue tracker projection

This SQLite example combines five workspace inputs in one checkpointed
projection. Commands append canonical issue facts, run the workspace member
under a keyed mutex, and answer from SQL. A same-owner change feed runs that
member in the background too.

```bash
bun run --cwd examples/issue-tracker start
bun run --cwd examples/issue-tracker smoke:http
bun run --cwd examples/issue-tracker app:full
ISSUE_TRACKER_DB=tracker.sqlite bun run --cwd examples/issue-tracker run:once acme
```

`ISSUE_TRACKER_DB` selects the SQLite file. `ISSUE_TRACKER_WORKSPACES` is the
comma-separated fixed member list and defaults to `acme,live`.

The application owns `issues`, `issue_labels`, `projects`, `users`, `labels`,
`issue_changes`, and `notification_drafts`. `@streamsy/projection` owns only
its `streamsy_projection_v1_*` tables. Both owners use the same `SqlClient`, so
application writes and checkpoint advancement share one transaction.
