# Timer stream

Run from the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run smoke:timer-stream
bun run --cwd examples/timer-stream test:unit
```

A State stream holds one-shot and cron schedule rows. An attached transactional
fire projection maintains a SQLite due table, appends fired facts and returns
`Projection.wakeAt(MIN(due_at))`. Many timer rows use one binding and one wake.
Cron uses UTC and catches up once: one overdue fact, then `Cron.next` after now.

The real-time smoke schedules a one-shot about one second ahead, cancels another
row and fires a six-field seconds cron. It checks that no fact fires early, the
cancelled row emits nothing, the one-shot is removed and cron advances. Both the
smoke and vitest check have an eight-second effect timeout. No network is needed.

The State stream records schedule changes; the due table owns active deadlines.
Firing does not rewrite source rows. Upserting a completed one-shot schedules it
again. This example's private due table is created for each run, not migrated.

Walkthrough: [Timer stream example](https://streamsy.dev/docs/examples/timer-stream).

Each run opens a fresh SQLite file in a scoped temporary directory under
`.data/`, closes the database and removes that directory on exit, including
failure. The examples use real time and require no server, Docker or credentials.
A persistent host would retain the database and initialize its application tables
before attaching runners. The package's `start` script runs the same finite flow.
