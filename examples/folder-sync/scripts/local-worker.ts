import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Config, Effect } from "effect";
import { startWorker } from "./worker-harness.ts";

const local = Effect.gen(function* () {
  const token = yield* Config.String("FOLDER_SYNC_WRITER_TOKEN");
  const port = yield* Config.Port("PORT").pipe(Config.withDefault(1339));

  const directory = yield* Config.String("FOLDER_SYNC_WORKER_STATE").pipe(
    Config.withDefault(`${import.meta.dir}/../.scratch/worker`),
  );

  const server = yield* Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, token, port)),
    (host) => Effect.promise(host.stop),
  );

  yield* Effect.logInfo(`Local folder-sync Worker: ${server.url}`);

  return yield* Effect.never;
});

BunRuntime.runMain(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable boundary owns the local runtime scope.
  local.pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);
