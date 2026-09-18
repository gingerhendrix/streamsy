# @streamsy/storage

SQLite storage for `@streamsy/core`, built on Effect SQL. One entry for Bun,
one for SQLite Durable Objects, and a driver-free root for your own
`SqlClient`. Version 0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/storage @streamsy/core effect
```

## Entries

| Entry                              | Use it for                                                          |
| ---------------------------------- | ------------------------------------------------------------------- |
| `@streamsy/storage/bun`            | A retained SQLite file on Bun (`@effect/sql-sqlite-bun`)            |
| `@streamsy/storage/durable-object` | SQLite inside a Cloudflare Durable Object (`@effect/sql-sqlite-do`) |
| `@streamsy/storage`                | Any SQLite-family `SqlClient` (SQLite 3.42 or newer) you provide    |

The Bun and Durable Object entries export `layer` and `layerProtocol`.
`layerProtocol` gives you the reader and writer services with the SQL
connection kept private. `layer` also exposes the `SqlClient` so your own SQL
can share the connection and commit with a stream mutation. The root entry
exports `layer` over a `SqlClient` you supply, plus `CommitBoundary`.

## Serve a SQLite-backed host on Bun

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

## Commit your SQL with a stream write

`CommitBoundary.withTransaction` runs application SQL and Streamsy mutations
in one transaction. Nested calls join the outer boundary. A rejected mutation
fails with `MutationRejected` and rolls the transaction back; catch it outside
the boundary. The [storage guide](https://streamsy.dev/docs/runtime/storage)
shows the complete pattern.

## Good to know

- Only fresh 0.4-format files are accepted. Older Streamsy files and newer
  unknown schemas are rejected without being changed.
- Bun defaults the client `busyTimeout` to zero and retries storage-owned
  transactions with a bounded yielding policy. Tune `client.busyTimeout`,
  `transactionRetryAttempts`, and `transactionRetryDelayMs` if you need to.
- Long-poll reads default to 30 seconds on Bun and 25 seconds in a Durable
  Object.
- Change notifications are process-local. A repair pass every `repairIntervalMs`
  (default 1,000 ms) bounds staleness across processes.

## License

MIT
