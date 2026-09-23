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
`layerProtocol` exposes `StreamsReader`, `StreamsWriter`, `Storage`,
`CommitBoundary`, `SqlClient`, and SQL reactivity on both hosts. Your own SQL
can share the connection and commit with a stream mutation. `layer` exposes
the same storage services without the protocol reader and writer. The root entry
exports `layer` over a `SqlClient` you supply, plus `CommitBoundary`.

## Serve a SQLite-backed host on Bun

```ts
import { Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { BunRuntime } from "@effect/platform-bun";
import { Http } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";
import { layerProtocol } from "@streamsy/storage/bun";

const Server = HttpRouter.serve(Http.routes({ prefix: "/streams" })).pipe(
  Layer.provide(layerProtocol({ client: { filename: "./streams.sqlite" } })),
  Layer.provide(listener({ port: 3000 })),
);
BunRuntime.runMain(Layer.launch(Server));
```

## Commit your SQL with a stream write

`CommitBoundary.withTransaction` runs application SQL and Streamsy mutations
in one transaction. Nested calls join the outer boundary. A rejected mutation
fails with `MutationRejected` and rolls the transaction back; catch it outside
the boundary. The [storage guide](https://streamsy.dev/docs/runtime/storage)
shows the complete pattern.

For a projection on that same connection:

```ts
import { Layer } from "effect";
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";

const host = ProjectionSqlite.layer.pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } })),
);
```

In a Durable Object, use `DurableObjectStorage.layerProtocol` with
`client: { storage: ctx.storage }` in the same composition.

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
