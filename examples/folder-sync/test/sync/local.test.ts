import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, FileSystem, Stream } from "effect";
import { testDirectory } from "../../scripts/test-directory.ts";
import { captureLocal } from "../../sync/local.ts";

layer(BunServices.layer)((it) => {
  for (const change of ["delete", "rename"] as const) {
    it.effect(`capture tolerates an editor ${change} after stat`, () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory(`sync-${change}-`);
        const fs = yield* FileSystem.FileSystem;
        const file = `${directory}/working`;
        yield* fs.makeDirectory(`${directory}/.folder-sync`, { recursive: true });
        yield* fs.writeFileString(file, "original bytes");

        const raced = {
          ...fs,
          stat: (target: string) =>
            fs
              .stat(target)
              .pipe(
                Effect.tap(() =>
                  target === file
                    ? change === "delete"
                      ? fs.remove(file)
                      : fs.rename(file, `${directory}/renamed`)
                    : Effect.void,
                ),
              ),
        };

        expect(
          yield* captureLocal(directory, "working").pipe(
            Effect.provideService(FileSystem.FileSystem, raced),
          ),
        ).toBeNull();

        if (change === "rename")
          expect(yield* fs.readFileString(`${directory}/renamed`)).toBe("original bytes");
      }),
    );
  }

  it.effect("capture tolerates disappearance before its immutable second read", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-second-read-");
      const fs = yield* FileSystem.FileSystem;
      const file = `${directory}/working`;
      yield* fs.makeDirectory(`${directory}/.folder-sync`, { recursive: true });
      yield* fs.writeFileString(file, "original bytes");
      let reads = 0;

      const raced = {
        ...fs,
        stream: (target: string, options?: Parameters<typeof fs.stream>[1]) => {
          if (target !== file || ++reads !== 2) return fs.stream(target, options);

          return Stream.unwrap(
            fs.rename(file, `${directory}/renamed`).pipe(Effect.as(fs.stream(target, options))),
          );
        },
      };

      expect(
        yield* captureLocal(directory, "working").pipe(
          Effect.provideService(FileSystem.FileSystem, raced),
        ),
      ).toBeNull();
      expect(reads).toBe(2);
      expect(yield* fs.readFileString(`${directory}/renamed`)).toBe("original bytes");
      expect(
        (yield* fs.readDirectory(`${directory}/.folder-sync/content`)).filter((name) =>
          name.startsWith(".capture-"),
        ),
      ).toEqual([]);
    }),
  );
});
