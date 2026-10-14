import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Config, Effect, Layer, Schedule } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";
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
  // Miniflare fires object alarms but not cron. This authenticated trigger calls the hourly scheduled pass.
  yield* HttpClient.post(`${server.url}/api/admin/wake`, {
    headers: { authorization: `Bearer ${token}` },
  }).pipe(
    Effect.tap((response) =>
      response.status === 200
        ? Effect.void
        : Effect.logError(`Local cron answered ${response.status}`),
    ),
    Effect.catch((cause) => Effect.logError("Local cron failed; retry next hour", cause)),
    Effect.repeat(Schedule.spaced("1 hour")),
  );
});

BunRuntime.runMain(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable boundary owns the local runtime and timer scope.
  local.pipe(Effect.scoped, Effect.provide(Layer.merge(BunServices.layer, FetchHttpClient.layer))),
);
