# @streamsy/storage

SQL storage for `@streamsy/core`, built on Effect SQL. One entry for Bun
SQLite, one for SQLite Durable Objects, and a driver-free root for your own
SQLite or Postgres `SqlClient`. Version 0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/storage @streamsy/core effect
```

## Entries

| Entry                              | Use it for                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------- |
| `@streamsy/storage/bun`            | A retained SQLite file on Bun (`@effect/sql-sqlite-bun`, `@effect/platform-bun`) |
| `@streamsy/storage/durable-object` | SQLite inside a Cloudflare Durable Object (`@effect/sql-sqlite-do`)              |
| `@streamsy/storage`                | A SQLite (3.42 or newer), Postgres or MySQL `SqlClient` you provide              |

The Bun and Durable Object entries export `layer` and `layerProtocol`.
`layerProtocol` exposes `StreamsReader`, `StreamsWriter`, `Storage`,
`CommitBoundary`, `SqlClient`, and SQL reactivity on both hosts. Your own SQL
can share the connection and commit with a stream mutation. `layer` exposes
the same storage services without the protocol reader and writer. The root entry
exports `layer` over a `SqlClient` you supply and `clientLayer` to provide one.
`CommitBoundary` comes from `@streamsy/core`.

## Use your own client

`clientLayer` builds a client from its `make` function and provides it with the
`Reactivity` it captured, so the client and the storage share one change graph.
The root entry imports no driver. Add the one you use, at `4.0.0-rc.115`.

```ts
import { Layer, Redacted } from "effect";
import { PgClient } from "@effect/sql-pg";
import * as Storage from "@streamsy/storage";

const storage = Storage.layer(streams).pipe(
  Layer.provide(Storage.clientLayer(PgClient.make({ url: Redacted.make(url) }))),
);
```

On Postgres and MySQL, every mutation transaction first locks the rows of the
streams it writes (`SELECT ... FOR UPDATE`), so writers on many connections or
processes apply one after the other.

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
  Layer.provide(layer(streams, { client: { filename: "./streams.sqlite" } })),
  Layer.provide(listener({ port: 3000 })),
);
BunRuntime.runMain(Layer.launch(Server));
```

## Commit your SQL with a stream write

`CommitBoundary.withTransaction` from `@streamsy/core` runs application SQL and Streamsy mutations
in one transaction. Nested calls join the outer boundary. A rejected mutation
fails with `MutationRejected` and rolls the transaction back; catch it outside
the boundary. The [Layers and storage guide](https://streamsy.dev/docs/hosting)
shows the complete pattern.

On a server database, the stream row lock is taken by the first stream mutation
inside the transaction, not when the transaction starts. Your SQL before that
mutation runs before the lock. Storage's reads inside a mutation are safe on
every dialect: on MySQL they are locking reads (`FOR UPDATE` or `FOR SHARE`),
so they see the latest committed rows even when your SQL fixed the
transaction's snapshot first. A stale plan is rejected, never written.

The protocol plans an append from `storage.record` and `storage.producer`.
Inside a transaction on MySQL, these reads are `FOR UPDATE` too, so each replan
sees the latest tail and the stream row stays locked until your transaction
ends. Other writers to that stream wait for your commit. Outside a transaction
they are plain reads and take no lock.

Your reads of your own tables are your job. On Postgres each statement reads
the latest committed rows. On MySQL at the default REPEATABLE READ, a plain
read uses the snapshot of the transaction's first plain read, which can be
older than the stream lock. If a decision depends on a row another writer can
change, read it with `FOR UPDATE` or `FOR SHARE` on MySQL.

For a projection on that same connection:

```ts
import { Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import * as BunStorage from "@streamsy/storage/bun";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } })),
);
```

`CheckpointStore.layer` keeps checkpoints in the storage table
`streamsy_owner_records`. `Transaction.layer` gives the SQL-only `Transaction`
service. `Commit.transaction` uses them to commit SQL, same-owner stream
appends, and the checkpoint together.

In a Durable Object, use `DurableObjectStorage.layerProtocol` with
`client: { storage: ctx.storage }` in the same composition.

## Good to know

- The Bun entry creates the parent folder of a new file. A read-only open, or
  `create: false`, creates nothing.
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
