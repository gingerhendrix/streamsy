# @streamsy/storage

SQL storage for `@streamsy/core`, built on Effect SQL. One entry for Bun
SQLite, one for SQLite Durable Objects, and a driver-free root for your own
SQLite or Postgres `SqlClient`. Version 0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/storage @streamsy/core effect
```

## Entries

| Entry                              | Use it for                                                          |
| ---------------------------------- | ------------------------------------------------------------------- |
| `@streamsy/storage/bun`            | A retained SQLite file on Bun (`@effect/sql-sqlite-bun`)            |
| `@streamsy/storage/durable-object` | SQLite inside a Cloudflare Durable Object (`@effect/sql-sqlite-do`) |
| `@streamsy/storage`                | A SQLite (3.42 or newer) or Postgres `SqlClient` you provide        |

The Bun and Durable Object entries export `layer` and `layerProtocol`.
`layerProtocol` exposes `StreamsReader`, `StreamsWriter`, `Storage`,
`CommitBoundary`, `SqlClient`, and SQL reactivity on both hosts. Your own SQL
can share the connection and commit with a stream mutation. `layer` exposes
the same storage services without the protocol reader and writer. The root entry
exports `layer` over a `SqlClient` you supply, `clientLayer` to provide one, and
`CommitBoundary`.

## Use your own client

`clientLayer` builds a client from its `make` function and provides it with the
`Reactivity` it captured, so the client and the storage share one change graph.
The root entry imports no driver. Add the one you use, at `4.0.0-rc.115`.

```ts
import { Layer, Redacted } from "effect";
import { PgClient } from "@effect/sql-pg";
import * as Storage from "@streamsy/storage";

const storage = Storage.layer({}, streams).pipe(
  Layer.provide(Storage.clientLayer(PgClient.make({ url: Redacted.make(url) }))),
);
```

On Postgres, every mutation transaction first locks the rows of the streams it
writes (`SELECT ... FOR UPDATE`), so writers on many connections or processes
apply one after the other.

## Serve a SQLite-backed host on Bun

```ts
import { Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { BunRuntime } from "@effect/platform-bun";
import { Http, Streamsy } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";
import { layer } from "@streamsy/storage/bun";

const streams = Streamsy.group(Streamsy.Wildcard);
const Server = HttpRouter.serve(Http.routes(streams, { prefix: "/streams", access: "full" })).pipe(
  Layer.provide(layer({ client: { filename: "./streams.sqlite" } }, streams)),
  Layer.provide(listener({ port: 3000 })),
);
BunRuntime.runMain(Layer.launch(Server));
```

## Commit your SQL with a stream write

`CommitBoundary.withTransaction` runs application SQL and Streamsy mutations
in one transaction. Nested calls join the outer boundary. A rejected mutation
fails with `MutationRejected` and rolls the transaction back; catch it outside
the boundary. The [Layers and storage guide](https://streamsy.dev/docs/hosting)
shows the complete pattern.

On a server database, the stream row lock is taken by the first stream mutation
inside the transaction, not when the transaction starts. Your SQL before that
mutation runs before the lock. On Postgres this is safe: each statement reads
the latest committed rows. On MySQL at the default REPEATABLE READ, a read
before the lock fixes the transaction's snapshot, and storage's own reads after
the lock can then miss another writer's commit. Until that is fixed, do not
commit your own SQL with stream writes on MySQL.

For a projection on that same connection:

```ts
import { Layer } from "effect";
import * as BunStorage from "@streamsy/storage/bun";
import * as ProjectionSql from "@streamsy/projection/sql";

const host = ProjectionSql.layerStorage.pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } })),
);
```

`ProjectionSql.layerStorage` provides `CheckpointStore` and the SQL-only
`Transaction` service. `Commit.transaction` uses them to commit SQL, same-owner
stream appends, and the checkpoint together.

In a Durable Object, use `DurableObjectStorage.layerProtocol` with
`client: { storage: ctx.storage }` in the same composition.

## Good to know

- Only files in the current storage format are accepted. Older Streamsy files,
  including those from earlier 0.4 builds, and newer unknown schemas are
  rejected without being changed. Use a fresh database path.
- Stream ids are limited to 512 characters, and producer ids to 255. A longer
  id fails with a non-retryable `StorageFault` before anything is written.
- Bun defaults the client `busyTimeout` to zero and retries storage-owned
  transactions with a bounded yielding policy. Tune `client.busyTimeout`,
  `transactionRetryAttempts`, and `transactionRetryDelayMs` if you need to.
- Long-poll reads default to 30 seconds on Bun and 25 seconds in a Durable
  Object.
- Change notifications are process-local. A repair pass every `repairIntervalMs`
  (default 1,000 ms) bounds staleness across processes.

## License

MIT
