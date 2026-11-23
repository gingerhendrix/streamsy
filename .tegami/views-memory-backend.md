---
packages:
  "@streamsy/views": minor
---

## Choose the Views backend when building a circuit host

**Breaking change:** `View.circuit` now runs on `OwnerTransaction` and requires a `ViewBackend`. SQL hosts provide both layers over the storage layer so the circuit uses the same SQL client as the storage owner.

```ts
import { Layer } from "effect";
import { CheckpointStore, OwnerTransaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { ViewBackend } from "@streamsy/views";

const host = Layer.mergeAll(
  CheckpointStore.layer,
  OwnerTransaction.layer,
  ViewBackend.layerSql,
).pipe(Layer.provideMerge(BunStorage.layer(group, { client })));
```

Keep calling `View.migrate(sql)` at startup. Keep `Transaction.layer` on any host that uses `StateTable`. Code that names a circuit's commit type now uses `Commit.OwnerCommit`.

For in-memory streams, compose `CheckpointStore.layer`, `OwnerTransaction.layer`, and `ViewBackend.layerMemory` over `MemoryStorage.layer(group)`. Circuit checkpoints, operator state, and output streams commit together in memory. This host needs no SQL client or migration. [Views](https://streamsy.dev/docs/views)
