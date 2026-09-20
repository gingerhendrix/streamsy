# Parked examples

Historical examples kept for reference. They are excluded from the root
workspace, builds, typechecks, lint, format, and tests, and they import
package entries that no longer exist. Each was last green at `0d895c2`.

| Directory                            | Disposition                                                            |
| ------------------------------------ | ---------------------------------------------------------------------- |
| `examples/issue-tracker`             | Slice 1 is restored; slices 2 and 3 wait for phases 5 and 6.          |
| `examples/issue-tracker-projections` | Retired. Its three claims (stateful single-source fold, proven coverage, dynamic fan-in board) moved to `examples/issue-tracker`; the per-issue stream family and the fan-in kernel were dropped on purpose. |
| `examples/risk-demo`                 | Restore the board in phase 4 and rebuild the complete demo in phase 6. |

To revive one, check out `0d895c2` for the working version, then port it to
the `0.4.0` entries: `@streamsy/serve` replaces `@streamsy/sinks`, and the
official `@durable-streams/state/db` binding replaces `@streamsy/tanstack-db`.
The restored demos in `examples/` are the pattern to copy.
