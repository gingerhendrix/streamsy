import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Config, Effect, Option } from "effect";
import { startWorker } from "./worker-harness.ts";

/**
 * Serve the built Worker on local workerd. State lives in WORKER_STATE, which
 * defaults to a disposable folder under the package's ignored `dist/`.
 */
const local = Effect.gen(function* () {
  const port = yield* Config.Port("PORT").pipe(Config.withDefault(8791));
  const directory = yield* Config.String("WORKER_STATE").pipe(
    Config.withDefault(`${import.meta.dir}/../dist/worker-state`),
  );
  const defenseTimeoutMs = yield* Config.Int("DEFENSE_TIMEOUT_MS").pipe(Config.option);

  const host = yield* Effect.acquireRelease(
    Effect.promise(() =>
      startWorker(directory, { port, defenseTimeoutMs: Option.getOrUndefined(defenseTimeoutMs) }),
    ),
    (server) => Effect.promise(server.stop),
  );

  yield* Effect.logInfo(`Local Hex Domination Worker: ${host.url}`);
  return yield* Effect.never;
});

// oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable owns the local runtime.
BunRuntime.runMain(local.pipe(Effect.scoped, Effect.provide(BunServices.layer)));
