# @streamsy/storage

SQL storage for `@streamsy/core`, built on Effect SQL. One entry for Bun
SQLite, one for SQLite Durable Objects, and a driver-free root for your own
SQLite, Postgres or MySQL `SqlClient`. Requires `effect@4.1.2`.

```sh
bun add @streamsy/storage @streamsy/core effect
```

## Entries

| Entry                              | Use it for                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------- |
| `@streamsy/storage/bun`            | A retained SQLite file on Bun (`@effect/sql-sqlite-bun`, `@effect/platform-bun`) |
| `@streamsy/storage/durable-object` | SQLite inside a Cloudflare Durable Object (`@effect/sql-sqlite-do`)              |
| `@streamsy/storage`                | A SQLite (3.42 or newer), Postgres or MySQL `SqlClient` you provide              |

The Bun and Durable Object entries export `BunStorage` and
`DurableObjectStorage`, each with `layer` and `layerProtocol`.
`layerProtocol` exposes `StreamsReader`, `StreamsWriter`, `Storage`,
`CommitBoundary`, `SqlClient`, and SQL reactivity on both hosts. Your own SQL
can share the connection and commit with a stream mutation. `layer` exposes
the same storage services without the protocol reader and writer. The root entry
exports `SqlStorage`, with `layer` over a `SqlClient` you supply and
`clientLayer` to provide one.
`CommitBoundary` comes from `@streamsy/core`.

## Use your own client

`SqlStorage.clientLayer` builds a client from its `make` function and provides it with the
`Reactivity` it captured, so the client and the storage share one change graph.
The root entry imports no driver. Add the one you use, at `4.1.2`.

```ts
import { Layer, Redacted } from "effect";
import { PgClient } from "@effect/sql-pg";
import { SqlStorage } from "@streamsy/storage";

const storage = SqlStorage.layer(streams).pipe(
  Layer.provide(SqlStorage.clientLayer(PgClient.make({ url: Redacted.make(url) }))),
);
```

On Postgres and MySQL, every mutation transaction first locks the rows of the
streams it writes (`SELECT ... FOR UPDATE`), so writers on many connections or
processes apply one after the other.

## Serve a SQLite-backed host on Bun

```ts
import { Layer } from "effect";
import { HttpRouter } from "effect/http";
import { BunRuntime } from "@effect/platform-bun";
import { Http, Streamsy } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";

const streams = Streamsy.group(Streamsy.Wildcard);
const Server = HttpRouter.serve(Http.routes(streams, { prefix: "/streams", access: "full" })).pipe(
  Layer.provide(BunStorage.layer(streams, { client: { filename: "./streams.sqlite" } })),
  Layer.provide(listener({ port: 3000 })),
);
BunRuntime.runMain(Layer.launch(Server));
```

## Commit your SQL with a stream write

`CommitBoundary.withTransaction` from `@streamsy/core` runs application SQL and Streamsy
mutations on the same client in one transaction. Nested calls on the same `SqlClient` join
without a savepoint, including through `sql.withoutTransforms()`. With two boundaries over one
client, only the outer boundary's hooks and `committed` subscribers see the commit.

A nested boundary on another SQL client opens its own transaction, as does a lone
`Storage.mutate` on that client. The inner transaction commits first and survives an
outer rollback. Use one client per database for nested work. With two clients on one
database, SQLite can wait until it reports a busy lock. Postgres or MySQL can wait for a
row lock that the outer transaction holds.

Durable Object clients share the driver's transaction, so a second client joins the first.
Use only one object's storage on a fiber. `MemoryStorage` dies when work enters another
`MemoryStorage` owner.

Open the boundary first. A raw transaction on that client may be nested inside it.
A boundary call or mutation in a raw `sql.withTransaction` without a matching
`CommitBoundary` dies before writing. A rejected mutation fails with `MutationRejected`
and rolls back its transaction when the failure escapes the boundary. Catch it outside the
boundary. The [Use SQLite guide](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables)
shows the complete pattern.

On a server database, the stream row lock is taken by the first stream mutation
inside the transaction, not when the transaction starts. Your SQL before that
mutation runs before the lock. On MySQL, storage uses locking reads
(`FOR UPDATE` or `FOR SHARE`) inside a mutation to see the latest committed
rows even under a caller-selected isolation level. Storage rejects a stale plan.

The protocol plans an append from `storage.record` and `storage.producer`.
Inside a transaction on MySQL, these reads are `FOR UPDATE` too, so each replan
sees the latest tail and the stream row stays locked until your transaction
ends. Other writers to that stream wait for your commit. Outside a transaction
they are plain reads and take no lock.

On MySQL the boundary selects `READ COMMITTED` for each transaction it opens,
overriding the session default, including `SERIALIZABLE`. This applies even
when the session inherits its default from the server or its pool sets one.
Other transactions use the session default unless the caller selects another
level. This includes transactions that projection marks open directly through
`SqlClient`. Marks written by a commit hook join the boundary transaction.
Keep the session in autocommit so the boundary can set the isolation before
beginning.

At `READ COMMITTED`, also the Postgres default, each plain read takes a fresh
snapshot of committed rows. `FOR UPDATE` and `FOR SHARE` can hold existing
rows stable until commit. On MySQL at this level, a locking read cannot lock an
absent row or guard a range against inserts. Enforce uniqueness with a unique
key or have writers lock an existing parent row.

At this level, `Projection.bindings` inside a MySQL transaction takes shared
locks on existing owner records it reads. Another host can commit a new
binding of the same projection before the listing transaction finishes.

To use the MySQL session's default, provide Effect's
`SqlClient.BeginTransaction` with the value `"START TRANSACTION"`. Opting out
to `REPEATABLE READ` can produce stale Views results when another host advances
the binding inside your transaction's snapshot. It can also cause
`Projection.forget` to miss a routed binding's first checkpoint when another
host commits it after the forgetting transaction's first read.

The boundary sends `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` and
`START TRANSACTION` in one request. A custom client with multi-statement
requests disabled fails at its first storage transaction. Effect's mysql2
client enables them by default. The mysql2 `-MULTI_STATEMENTS` flag disables
them.

With binary logging enabled, use `binlog_format=ROW` or `MIXED`. MySQL logs
InnoDB writes in these transactions by row in `MIXED` mode. See [MySQL
transaction isolation levels](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html).

For a projection on that same connection:

```ts
import { Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } })),
);
```

`CheckpointStore.layer` keeps checkpoints in the storage table
`streamsy_owner_records`. On Postgres and MySQL, a checkpoint save locks the
record it read, and a first save is a plain insert, so two concurrent saves of
one projection never both win. `Transaction.layer` provides `Transaction` and
`OwnerTransaction` from the same storage owner. `Commit.transaction` uses them to commit SQL, same-owner stream
appends, and the checkpoint together.
A projection unit on a second SQL client
opens its own transaction under the nesting rules above.
Build `Transaction.layer` and `CheckpointStore.layer` over the same SQL client.
With separate clients, the checkpoint commits on its own and can survive a rollback
of the projection's transaction.

In a Durable Object, use `DurableObjectStorage.layerProtocol` with
`client: { storage: ctx.storage }` in the same composition.

## Good to know

- The Bun entry creates the parent folder of a new file. A read-only open, or
  `create: false`, creates nothing.
- Only files in the current storage format are accepted. Older Streamsy files,
  including those from earlier 0.4 builds, and newer unknown schemas are
  rejected without being changed. Use a fresh database path.
- Migration 3 adds explicit stops, last-failure fields, and a nullable `wake_at`
  field for timers to projection marks. Sequence numbers, attempts, and due times
  survive the upgrade from version 2. A schema version 2 release cannot reopen
  the upgraded database.
- Storage migrates SQLite and Postgres to schema version 3 and MySQL to version 4.
  Stop every 0.13.0 process before opening a MySQL database with storage that
  upgrades it to version 4. See [SQL storage migrations](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).
- Stream ids are limited to 512 code points, and producer ids to 255. A longer
  id fails with a non-retryable `StorageFault` before anything is written.
- The checkpoint store adds no length limit to projection checkpoint keys.
  SQLite and Postgres keep the full key in the lookup column. On MySQL, owner
  record lookup keys longer than 512 code points use the first
  447 code points, `#`, and the SHA-256 hex of the full key. Storage saves the
  original key in `full_key`. A listing range that contains a legacy hashed key
  without its full key fails until the record is rewritten with the original key.
- On MySQL, durable projection ids are limited to 255 Unicode code points.
  Storage saves original pending mark params in `full_params` so they can exceed
  the 512-code-point lookup column.
- Bun defaults the client `busyTimeout` to zero and retries storage-owned
  transactions with a bounded yielding policy. Tune `client.busyTimeout`,
  `transactionRetryAttempts`, and `transactionRetryDelayMs` if you need to.
- Long-poll reads default to 30 seconds on Bun and 25 seconds in a Durable
  Object.
- Change notifications are process-local. A repair pass every `repairIntervalMs`
  (default 1,000 ms) bounds staleness across processes.

## License

MIT
