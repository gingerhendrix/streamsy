# @streamsy/storage

Effect SQL storage for `@streamsy/core`. The root entry is driver-package-free and
requires a SQLite-family `SqlClient` (SQLite 3.42 or newer) plus its shared `Reactivity` service. The
`/bun` and `/durable-object` entries construct the official rc.112 SQLite
drivers and provide both the existing core `Storage` tag and `CommitBoundary`.

```ts
import { layerProtocol } from "@streamsy/storage/bun";
import * as BunHost from "@streamsy/serve/bun";

const host = await BunHost.serve({
  layer: layerProtocol({ client: { filename: "./streamsy.sqlite" } }),
  port: 3000,
});
await host.stop();
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

`withTransaction` is deliberately low-level: a `Rejected` mutation outcome is a
successful Effect value and does not roll back application SQL by itself. To make
rejection atomic, raise a private typed error inside the boundary and recover it
only outside `withTransaction`; the site SQL storage guide contains the complete
pattern.

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
