# Parked examples

Historical examples kept for reference. They are excluded from the root
workspace, builds, typechecks, lint, format, and tests, and they import
package entries that no longer exist. Each was last green at `0d895c2`.

| Directory                            | Disposition                                                            |
| ------------------------------------ | ---------------------------------------------------------------------- |
| `examples/issue-tracker`             | Restore in phase 3 slice 1, phase 5 slice 2, and phase 6 slice 3.     |
| `examples/issue-tracker-projections` | Retired under decision 3; phase 3 records the final disposition.      |
| `examples/risk-demo`                 | Restore the board in phase 4 and rebuild the complete demo in phase 6. |

To revive one, check out `0d895c2` for the working version, then port it to
the `0.4.0` entries: `@streamsy/serve` replaces `@streamsy/sinks`, and the
official `@durable-streams/state/db` binding replaces `@streamsy/tanstack-db`.
The restored demos in `examples/` are the pattern to copy.
