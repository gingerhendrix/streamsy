# Command stream

Run from the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run smoke:command-stream
bun run --cwd examples/command-stream test:unit
```

A client submits the same caller key twice through `Projection.decide`. An attached
`Projection.process` writes one SQLite result row and publishes its `accepted`
receipt in the checkpoint transaction. The client waits with `Streamsy.awaitWake`.
A `Projection.wake` publishes changes consumed through the worker's `triggers`.
The smoke asserts one request, one effect and one receipt.

The request-key fold retains every key; use an application retention policy for a
large ledger. An accepted row describes input progress, so several commands in a
unit share one receipt. Reusing a key with different text retains the first request.

Guide: [Command streams](https://streamsy.dev/docs/projections/command-streams).
Walkthrough: [Command stream example](https://streamsy.dev/docs/examples/command-stream).

Each run opens a fresh SQLite file in a scoped temporary directory under
`.data/`, closes the database and removes that directory on exit, including
failure. The examples use real time and require no server, Docker or credentials.
A persistent host would retain the database and initialize its application tables
before attaching runners. The package's `start` script runs the same finite flow.
