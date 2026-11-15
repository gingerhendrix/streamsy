import { BunFileSystem } from "@effect/platform-bun";
import { Effect, FileSystem, Layer } from "effect";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";

/** Scope cleanup closes SQLite before removing its temporary directory. */
export const Storage = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const parent = new URL("../.data/", import.meta.url).pathname;
    yield* fs.makeDirectory(parent, { recursive: true });
    const directory = yield* fs.makeTempDirectoryScoped({ directory: parent, prefix: "run-" });

    return Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(
        BunStorage.layerProtocol({ client: { filename: `${directory}/streams.sqlite` } }),
      ),
    );
  }),
).pipe(Layer.provide(BunFileSystem.layer));
