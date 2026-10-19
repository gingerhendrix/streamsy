/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- Each Bun test is an Effect entry point. */
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunPath from "@effect/platform-bun/BunPath";
import { expect, test } from "vitest";
import { Cause, Effect, Exit, FileSystem, Layer, Option, Path } from "effect";
import { BunStorage } from "@streamsy/storage/bun";

const Platform = Layer.mergeAll(BunFileSystem.layer, BunPath.layer);

/** Run `body` with a fresh temp folder that is removed afterwards. */
const inTempFolder = <A, E>(
  body: (
    folder: string,
    fs: FileSystem.FileSystem,
    path: Path.Path,
  ) => Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const folder = yield* fs.makeTempDirectoryScoped({ prefix: "streamsy-sqlite-file-" });

        return yield* body(folder, fs, path);
      }),
    ).pipe(Effect.provide(Platform)),
  );

const open = (client: {
  readonly filename: string;
  readonly readonly?: boolean;
  readonly create?: boolean;
}) => Effect.scoped(Layer.build(BunStorage.layerProtocol({ client }))).pipe(Effect.exit);

const failure = (exit: Exit.Exit<unknown, unknown>) => {
  if (Exit.isSuccess(exit)) throw new Error("Expected the SQLite open to fail");

  return Option.getOrThrow(Cause.findErrorOption(exit.cause));
};

test("a new file in a missing folder creates the folder", async () => {
  await inTempFolder((folder, fs, path) =>
    Effect.gen(function* () {
      const filename = path.join(folder, "x", "y", "db.sqlite");
      const exit = yield* open({ filename });
      expect(Exit.isSuccess(exit)).toBe(true);
      expect(yield* fs.exists(path.join(folder, "x", "y"))).toBe(true);
      expect(yield* fs.exists(filename)).toBe(true);
    }),
  );
});

test("a read-only or no-create open does not create the folder", async () => {
  await inTempFolder((folder, fs, path) =>
    Effect.gen(function* () {
      const filename = path.join(folder, "x", "y", "db.sqlite");

      for (const mode of [{ readonly: true }, { create: false }]) {
        const exit = yield* open({ filename, ...mode });
        expect(failure(exit)).toMatchObject({
          _tag: "StorageFault",
          operation: "migration.open",
          retryable: false,
        });
        expect(yield* fs.exists(path.join(folder, "x"))).toBe(false);
      }
    }),
  );
});

test("a folder that cannot be created fails with a StorageFault", async () => {
  await inTempFolder((folder, fs, path) =>
    Effect.gen(function* () {
      const blocker = path.join(folder, "blocker");
      yield* fs.writeFileString(blocker, "not a folder");
      const exit = yield* open({ filename: path.join(blocker, "db.sqlite") });
      expect(failure(exit)).toMatchObject({
        _tag: "StorageFault",
        operation: "migration.open",
        message: expect.stringContaining("Cannot create the folder"),
        retryable: false,
      });
    }),
  );
});
