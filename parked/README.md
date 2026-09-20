# Parked examples

Historical examples kept for reference. They are excluded from the root
workspace, builds, typechecks, lint, format, and tests, and they import
package entries that no longer exist. Each was last green at `0d895c2`.

| Directory                                | What it showed                                                |
| ---------------------------------------- | ------------------------------------------------------------- |
| `examples/issue-tracker-projections`     | Per-issue projections and a fan-in board on the old mesh, with Alchemy |
| `examples/issue-tracker`                 | The full stack: views, three sinks, an action outbox, Bun and Cloudflare hosts |
| `examples/risk-demo`                     | An event-sourced game with per-generation board projections and SSE |

To revive one, check out `0d895c2` for the working version, then port it to
the `0.4.0` entries: `@streamsy/serve` replaces `@streamsy/sinks`, and the
official `@durable-streams/state/db` binding replaces `@streamsy/tanstack-db`.
