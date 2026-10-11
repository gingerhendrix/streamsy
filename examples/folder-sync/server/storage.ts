import { MemoryStorage } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunCrypto } from "@effect/platform-bun";
import { Effect, Layer, Option } from "effect";
import { memoryBlobs, sqliteBlobs } from "./blob-store.ts";
import { ServerConfig } from "./config.ts";
import { streams } from "./streams.ts";

export const memoryHost = Layer.unwrap(
  Effect.sync(() => {
    const storage = MemoryStorage.layer(streams, { readLimit: 1 });

    return Layer.merge(CheckpointStore.layer, memoryBlobs).pipe(
      Layer.provideMerge(storage),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

export const sqliteHost = (filename: string) => {
  const storage = BunStorage.layer(streams, { client: { filename }, readLimit: 1 });

  return Layer.mergeAll(CheckpointStore.layer, Transaction.layer, sqliteBlobs).pipe(
    Layer.provideMerge(storage),
    Layer.provideMerge(BunCrypto.layer),
  );
};

export const StorageLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ databasePath }) =>
    Option.match(databasePath, { onNone: () => sqliteHost(":memory:"), onSome: sqliteHost }),
  ),
);
