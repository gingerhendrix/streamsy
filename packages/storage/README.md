# @streamsy/storage

Effect SQL storage for `@streamsy/core`. The root entry is driver-package-free and
requires a SQLite-family `SqlClient` (SQLite 3.42 or newer) plus its shared `Reactivity` service. The
`/bun` and `/durable-object` entries construct the official rc.112 SQLite
drivers and provide both the existing core `Storage` tag and `CommitBoundary`.
The Bun `layer` also exposes that exact `SqlClient` and `Reactivity` graph so
application SQL can participate in `CommitBoundary.withTransaction` without a
second connection. `layerProtocol` keeps those implementation services private.
The `/durable-object` entry exports the same protocol composition for local Durable
Objects and defaults long-poll reads to 25 seconds; the Bun entry keeps its existing
30-second default.

```ts
import { Effect } from "effect";
import { layerProtocol } from "@streamsy/storage/bun";
import { start } from "@streamsy/serve/bun";

const host = await Effect.runPromise(
  start({
    layer: layerProtocol({ client: { filename: "./streamsy.sqlite" } }),
    port: 3000,
  }),
);
await Effect.runPromise(host.stop);
```

The Bun entry defaults the synchronous driver `busyTimeout` to zero. Standalone
storage-owned transactions make 16 total attempts with a fixed 25 ms yielding
delay (375 ms maximum scheduled delay). Override these with an explicit client
`busyTimeout`, `transactionRetryAttempts` or `transactionRetryDelayMs` only after
accounting for event-loop and shutdown latency.
The read-only format preflight, WAL preparation and schema migration use the
same bounded yielding policy during scoped Layer acquisition.

Use `CommitBoundary.withTransaction` when application SQL and a Streamsy
mutation must commit together. Nested boundary calls join the outer boundary.
A boundary or storage mutation inside a foreign raw `SqlClient.withTransaction`
defects before its body/storage SQL executes; raw transactions receive no
commit notification guarantee.

`Storage.mutate` succeeds only with `Applied` operation results. An expected
rejection fails with `MutationRejected` and rolls back application SQL when it
escapes `withTransaction`. Recover with `Effect.catchTag("MutationRejected", ...)`
outside the boundary, after rollback. Catching it inside the outer body permits
that body’s other writes to commit; nested calls do not create savepoints. The
site SQL storage guide contains the complete pattern.

A standalone rejected mutation publishes no invalidations and is never retried
as a SQL fault. Only retryable `StorageFault` or driver `SqlError` failures use
the existing bounded transaction retry policy.

`CommitBoundary.withTransaction` belongs to its caller and is not automatically
retried. A storage mutation nested inside it does not add its own retry loop.
Commit and rollback failures remain defects and are never replayed because a
failed commit acknowledgement can hide a successful commit.

`Storage.changes` keeps one capacity-1 payload-free dropping queue per active
subscriber. It registers before the initial read and performs an authoritative
SQL reread after each local wake. A scoped repair pass runs every 1,000 ms by
default; this is the cross-process staleness bound and also covers interruption
between commit and local invalidation. Set `repairIntervalMs` to a positive
finite value to choose another operational bound. Tests use a 5-second outer
assertion timeout around real-time notification schedules.

Only fresh 0.4-format files are accepted. A pre-0.4 Streamsy file or a database
with a newer unsupported schema is rejected without schema mutation. No import,
reset, deletion, or legacy offset translation is performed. New-format migration
state is recorded in `streamsy_storage_schema_version`.
