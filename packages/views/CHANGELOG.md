## @streamsy/views@0.14.0

### Use `generation` instead of `version` on projections and Views circuits

**Breaking change:** Projection and Views circuit declarations now use `generation` alone to version stored state and rebuilds, and reject any defined `version` with `RangeError`.

- **Declarations:** Remove `version: 1` to keep the existing generation. For any other version, remove `version` and raise `generation` above every generation the `id` has previously used.
- **Types and custom stores:** `ProjectionKey`, `Unit`, and the derived declaration and binding types no longer have a `version` field. If custom stores or handlers included `key.version` or `unit.version` in storage keys, substitute the literal `1` to preserve keys for the former default version. Passing a key object with a defined `version` other than `1` to `recordKey`, `Projection.forget`, or a `fromStore` checkpoint store throws `RangeError`.
- **Unchanged:** `View.fn(name, version, implementation)` keeps its function version argument.

When upgrading a declaration that used a `version` other than `1`, stop the old runners, retain full input history, and prepare output addresses before starting the upgraded process. Let the new generation catch up and move readers before retiring the old binding with a one-off 0.13.0 script that only calls `retire`. See [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version).

### Run Views on Postgres, MySQL, and in memory

`@streamsy/views` now supports PostgreSQL, MySQL, and in-memory `MemoryStorage` hosts alongside SQLite and Cloudflare Durable Objects.

**Breaking change:** `View.circuit` now requires a `ViewBackend` service. Provide `ViewBackend.layerSql` (for SQLite, Postgres, or MySQL) or `ViewBackend.layerMemory` (for `MemoryStorage`) alongside `CheckpointStore.layer` and `Transaction.layer` over the same storage layer. Code that names a circuit's commit type now uses `Commit.TransactionCommit`.

```ts
import { Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { ViewBackend } from "@streamsy/views";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer, ViewBackend.layerSql).pipe(
  Layer.provideMerge(BunStorage.layer(group, { client })),
);
```

- **SQLite and Durable Objects:** Keep calling `View.migrate(sql)` at startup. If the host also uses `StateTable`, call `StateTable.migrate(sql)`. See [Choose a host](https://streamsy.dev/docs/views/declare-and-run#choose-a-host).
- **Postgres and MySQL:** Compose `CheckpointStore.layer`, `Transaction.layer`, and `ViewBackend.layerSql` over `SqlStorage`, and run `View.migrate(sql)` at host startup (safe across concurrent hosts). On MySQL (tested on MySQL 8.4.11), install `@streamsy/storage` at the same version for the `READ COMMITTED` transaction isolation Views relies on. Neither dialect imposes a length limit on row keys or sort keys, but snapshot arrangement identifiers must fit in 255 UTF-8 bytes (including prefix, encoded State key, and collection name); an overlong identifier or a row/owner key containing a lone surrogate fails the unit with a non-retryable `ViewDataFault`. See [Postgres](https://streamsy.dev/docs/views/postgres) and [MySQL](https://streamsy.dev/docs/views/mysql).
- **In memory:** Compose `CheckpointStore.layer`, `Transaction.layer`, and `ViewBackend.layerMemory` over `MemoryStorage.layer(group)`. Checkpoints, operator state, and output streams commit together in memory with no SQL client or migration. See [Run Views in memory](https://streamsy.dev/docs/views/memory).

### Read live Views snapshots with `View.live`

Declare in-memory views with `View.live` and open a reader with `at(params)` inside an Effect scope. The reader maintains its own memory host and exposes `snapshot` (all committed views and their input offsets together), `snapshots` (a live stream of updates), and `caughtUp` (waits for the initial replay). Works in browsers, tests, or any Effect process—provide remote State services with `RemoteStreams` to follow server streams and compare committed input offsets against write offsets. See [Live snapshots](https://streamsy.dev/docs/views/live).

### Break row-key ties consistently across backends

When rows have equal sort terms, Views now breaks ties by encoded row key in SQLite order across all backends. This can change which rows `top` emits and how floating-point sums round when keys contain supplementary Unicode characters. SQLite keeps the same stored key bytes and table layout.

If an existing view already wrote affected results, raise its circuit's `generation` to rebuild from retained input history and switch readers once it catches up. See [Lifecycle](https://streamsy.dev/docs/views/lifecycle).

## @streamsy/views@0.13.0

### Maintain incremental SQL views with `@streamsy/views`

`@streamsy/views` is now published. Install it with `bun add @streamsy/views @streamsy/projection @streamsy/core effect`.

Build relations over `Streamsy.state` collections with `View.from`, then join, filter, select, group with `View.count`, `View.sum`, `View.min` and `View.max`, and take a bounded `top`. `View.circuit` compiles the relations into one projection. Each unit updates only the affected keys in SQLite operator tables, appends the changed rows to generation-addressed State output streams, and commits the input checkpoint in the same transaction. Wrap the output with `Projection.perGeneration` and create the tables with `View.migrate`.

Views supports SQLite only: Bun SQLite and Cloudflare Durable Object SQLite. `View.migrate` fails on Postgres and MySQL.

The package is pre-1.0. When you change a circuit's plan, raise its `generation` so the binding rebuilds into a fresh namespace and output stream. A circuit whose stored plan hash differs fails with `PlanChanged`. A later release that changes the operator-table layout will mark it as a breaking change, and you raise `generation` on every affected circuit when you upgrade. [Views](https://streamsy.dev/docs/views)
