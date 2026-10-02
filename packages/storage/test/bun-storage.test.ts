import { StorageContract } from "@streamsy/core/testing";
import { Layer } from "effect";
import { BunStorage } from "@streamsy/storage/bun";

StorageContract.run({
  name: "official Bun SQLite Storage contract",
  layer: BunStorage.layerProtocol({
    client: { filename: ":memory:", disableWAL: true, busyTimeout: "50 millis" },
    repairIntervalMs: 1_000,
  }).pipe(Layer.orDie),
  expected: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
  ownerRecords: true,
});
