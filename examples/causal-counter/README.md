# Causal counter

This headless example proves one claim:

> A source counter increment acknowledged at token `S` is visible in an eagerly materialized derived counter only when that consumer's lineage row proves coverage through `S`.

The source and target use distinct mesh identities and application stream ids. `appendCounterIncrement()`, `projectCounterIncrements()`, and the eager consumer's `catchUp()` return Effects. They compose through fixed bindings plus the finite read/append/recovery capabilities. One bounded projection writes deterministic counter-contribution `upsert` rows and the reserved lineage `upsert` in the same State append. The eager consumer explicitly registers those two collections, applies each delivered batch transactionally, and advances its target resume position only with the visible rows. Counter values are the sum of idempotently keyed contribution rows.

The executable and test hosts each construct one `ManagedRuntime`, reuse it for the host lifetime, and dispose it at the edge. No example domain operation creates a runtime or converts itself to a Promise.

`syncedThrough(ack)` reads the materialized lineage row and calls the pure causal coverage function. It never compares a target offset with a source offset. Before lineage exists it returns `not-yet`; a different source identity is `incomparable`; an acknowledgement beyond the watermark is `not-yet`.

The example uses a small example-local eager materializer because the existing StreamDB integration is browser/TanStack-oriented. This slice does not claim a reusable StreamDB adapter or generic sync engine.

Run the deterministic smoke and evidence suites with:

```sh
bun run smoke:causal-counter
bun run --cwd examples/causal-counter test:unit
bun run --cwd examples/causal-counter test:sqlite
```

## Scope and limitations

- Experimental evidence covers memory and SQLite, including SQLite reopen; direct and fetch clients are tested.
- Recovery scans output history and is O(history).
- The topology is one ordered source, one immutable output generation, and one fixed producer lane/epoch.
- Producer idempotency makes a same-tuple retry sequence-already-accepted; it does not verify payload equality and this example does not claim exactly-once processing.
- Expected-offset CAS detects a competing target writer; it does not provide ownership handoff or multi-writer orchestration.
- Filesystem and Durable Object hosts are not claimed.
- There is no router, subscription, snapshot, session projection, UI, lifecycle runtime, generic sync engine, or graduated package API here.
