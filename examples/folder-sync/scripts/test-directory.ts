import { Config, Effect, FileSystem } from "effect";

/** A caller can keep all test data in a dated scratch directory. Other checkouts use an ignored local directory. */
export const testDirectory = Effect.fn("FolderSync.testDirectory")(function* (prefix: string) {
  const directory = yield* Config.String("FOLDER_SYNC_SCRATCH").pipe(
    Config.withDefault(`${import.meta.dirname}/../.scratch`),
  );

  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(directory, { recursive: true });

  return yield* fs.makeTempDirectoryScoped({ directory, prefix });
});
