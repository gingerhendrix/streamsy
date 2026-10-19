import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, FileSystem, Logger } from "effect";
import { testDirectory } from "../../scripts/test-directory.ts";
import { skipWarnings } from "../../sync/local.ts";

layer(BunServices.layer)((it) => {
  it.effect("skipped paths warn once per metadata change in one daemon", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("warning-");
      const fs = yield* FileSystem.FileSystem;
      const file = `${directory}/large`;
      yield* fs.writeFileString(file, "one");
      const messages: unknown[] = [];
      const logger = Logger.make(({ message }) => messages.push(message));
      const warn = yield* skipWarnings;
      yield* Effect.gen(function* () {
        yield* warn(file, "skip");
        yield* warn(file, "skip");
        yield* fs.writeFileString(file, "changed size");
        yield* warn(file, "skip");
        yield* warn(file, "skip");
      }).pipe(Effect.provideService(Logger.CurrentLoggers, new Set([logger])));
      expect(messages).toHaveLength(2);
    }),
  );
});
