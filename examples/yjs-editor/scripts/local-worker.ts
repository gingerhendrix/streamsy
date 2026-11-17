import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Config, Effect } from "effect";
import { startWorker } from "./worker-harness.ts";

const local = Effect.gen(function* () {
  const port = yield* Config.Port("PORT").pipe(Config.withDefault(1340));

  const directory = yield* Config.String("YJS_WORKER_STATE").pipe(
    Config.withDefault(`${import.meta.dir}/../.scratch/worker`),
  );

  const threshold = yield* Config.Number("COMPACTION_THRESHOLD").pipe(
    Config.withDefault(1024 * 1024),
  );

  const host = yield* Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, port, threshold)),
    (server) => Effect.promise(server.stop),
  );

  yield* Effect.logInfo(`Local Yjs Worker: ${host.url}`);

  return yield* Effect.never;
});

// oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable owns the local runtime.
BunRuntime.runMain(local.pipe(Effect.scoped, Effect.provide(BunServices.layer)));
