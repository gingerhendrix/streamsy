import { MemoryStorage } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunCrypto, BunServices } from "@effect/platform-bun";
import { Effect, FileSystem, Layer, Option } from "effect";
import { BlobStore, memoryBlobs } from "./blob-store.ts";
import { diskBlobs } from "./disk-blobs.ts";
import { ServerConfig } from "./config.ts";
import { streams } from "./streams.ts";

export const memoryHost = Layer.unwrap(
  Effect.sync(() => {
    const storage = MemoryStorage.layer(streams);

    return Layer.merge(CheckpointStore.layer, memoryBlobs).pipe(
      Layer.provideMerge(storage),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

export const sqliteHost = <E = never, R = never>(
  filename: string,
  blobLayer?: Layer.Layer<BlobStore, E, R>,
) => {
  const blobs =
    blobLayer ?? (filename === ":memory:" ? memoryBlobs : diskBlobs(`${filename}.blobs`));

  const storage = BunStorage.layer(streams, { client: { filename } });

  return Layer.mergeAll(CheckpointStore.layer, Transaction.layer, blobs).pipe(
    Layer.provideMerge(storage),
    Layer.provideMerge(BunCrypto.layer),
  );
};

export const StorageLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ databasePath }) =>
    Option.match(databasePath, {
      onNone: () =>
        sqliteHost(
          ":memory:",
          Layer.unwrap(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem;
              const parent = `${import.meta.dirname}/../.scratch`;
              yield* fs.makeDirectory(parent, { recursive: true });

              const directory = yield* fs.makeTempDirectoryScoped({
                directory: parent,
                prefix: "blobs-",
              });

              return diskBlobs(directory);
            }),
          ).pipe(Layer.provide(BunServices.layer)),
        ),
      onSome: sqliteHost,
    }),
  ),
);
