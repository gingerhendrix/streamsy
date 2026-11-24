---
packages:
  "@streamsy/views": minor
---

## Choose the Views backend when building a circuit host

**Breaking change:** `View.circuit` now requires a `ViewBackend`. Add `ViewBackend.layerSql` alongside `CheckpointStore.layer` and `Transaction.layer` over the same storage layer. [Upgrade an existing circuit host](https://streamsy.dev/docs/views/declare-and-run#upgrade-an-existing-circuit-host).

```ts
import { Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { ViewBackend } from "@streamsy/views";

const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer, ViewBackend.layerSql).pipe(
  Layer.provideMerge(BunStorage.layer(group, { client })),
);
```

Keep calling `View.migrate(sql)` at startup. If the SQLite host uses `StateTable`, also run `StateTable.migrate(sql)`. Code that names a circuit's commit type now uses `Commit.TransactionCommit`.

For in-memory streams, compose `CheckpointStore.layer`, `Transaction.layer`, and `ViewBackend.layerMemory` over `MemoryStorage.layer(group)`. Circuit checkpoints, operator state, and output streams commit together in memory. This host needs no SQL client or migration. [Run Views in memory](https://streamsy.dev/docs/views/memory)
