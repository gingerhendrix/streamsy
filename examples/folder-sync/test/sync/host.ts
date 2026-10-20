import { spawnServer as bunServer } from "@streamsy/test-support/server";
import { Config, Effect } from "effect";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";

export const token = "sync-test-token";

/** Reuse the same server-restart, mirror and delivery-race scenarios on both runtimes. */
export const spawnServer = (options: Parameters<typeof bunServer>[0]) =>
  Effect.gen(function* () {
    const worker = yield* Config.Boolean("FOLDER_SYNC_TEST_WORKER").pipe(Config.withDefault(false));

    if (!worker)
      return yield* bunServer({
        ...options,
        env: { ...options.env, FOLDER_SYNC_WRITER_TOKEN: token },
      });

    const directory =
      options.env?.FOLDER_SYNC_DB === undefined
        ? yield* testDirectory("worker-sync-")
        : `${options.env.FOLDER_SYNC_DB}.workerd`;

    const server = yield* Effect.acquireRelease(
      Effect.promise(() => startWorker(directory, token, options.port)),
      (host) => Effect.promise(host.stop),
    );

    return {
      url: server.url,
      port: Number(new URL(server.url).port),
      stop: Effect.promise(server.stop),
    };
  });
