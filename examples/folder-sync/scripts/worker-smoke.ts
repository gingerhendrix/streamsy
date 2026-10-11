import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { testDirectory } from "./test-directory.ts";
import { startWorker } from "./worker-harness.ts";

const smoke = Effect.gen(function* () {
  const directory = yield* testDirectory("worker-smoke-");
  const token = "local-smoke-token";

  const host = yield* Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, token)),
    (runtime) => Effect.promise(runtime.stop),
  );

  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const code = yield* spawner.exitCode(
    ChildProcess.make("bun", ["scripts/http-smoke.ts"], {
      cwd: `${import.meta.dir}/..`,
      env: { FOLDER_SYNC_URL: host.url, FOLDER_SYNC_WRITER_TOKEN: token },
      stdout: "inherit",
      stderr: "inherit",
    }),
  );

  if (code !== 0) return yield* Effect.die(`Worker HTTP smoke exited ${code}`);
  yield* Effect.logInfo("Worker HTTP smoke passes");

  return yield* Effect.void;
});

// oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable boundary owns the local runtime and child-process scope.
BunRuntime.runMain(smoke.pipe(Effect.scoped, Effect.provide(BunServices.layer)));
