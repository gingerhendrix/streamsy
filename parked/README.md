# Parked examples

Historical examples kept for reference. They are excluded from the root
workspace, builds, typechecks, lint, format, and tests, and they import
package entries that no longer exist. Each was last green at `0d895c2`.

| Directory                            | Disposition                                                            |
| ------------------------------------ | ---------------------------------------------------------------------- |
| `examples/issue-tracker`             | The SQLite projection is restored in `examples/issue-tracker`; serving and the full application remain parked.          |
| `examples/issue-tracker-projections` | Retired. Its three claims (stateful single-source fold, proven coverage, dynamic fan-in board) moved to `examples/issue-tracker`; the per-issue stream family and the fan-in kernel were dropped on purpose. |
| `examples/risk-demo`                 | The board reducer and persisted fold are restored in `examples/hex-board`; the command API, server, bot, Cloudflare host, SSE actions, UI and mesh remain parked for phase 6. |

To revive one, check out `0d895c2` for the working version, then port it to
the `0.4.0` entries: use `@streamsy/serve` for serving and the
official `@durable-streams/state/db` binding for browser collections.
The restored demos in `examples/` are the pattern to copy.
