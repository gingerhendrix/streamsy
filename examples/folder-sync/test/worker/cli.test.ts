import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Fiber, FileSystem, Layer, Schedule, Schema, Stream } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";

class Waiting extends Schema.TaggedError<Waiting>()("WorkerCliWaiting", {}) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 300 })),
    }),
  );

layer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect(
      "the real CLI overrides the environment on 401 and delivers its retained write after an env-token restart",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-cli-auth-");
          const fs = yield* FileSystem.FileSystem;
          const mirror = `${directory}/mirror`;
          yield* fs.makeDirectory(`${mirror}/docs`, { recursive: true });
          yield* fs.writeFileString(`${mirror}/docs/local.md`, "pending local bytes");

          const host = yield* Effect.acquireRelease(
            Effect.promise(() => startWorker(`${directory}/worker`, "correct-token")),
            (runtime) => Effect.promise(runtime.stop),
          );

          const created = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers: { authorization: "Bearer correct-token" },
            body: yield* HttpBody.json({ folderId: "docs", title: "Docs" }),
          });

          expect(created.status).toBe(200);
          yield* created.text;
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

          const cli = yield* spawner.spawn(
            ChildProcess.make(
              "bun",
              [
                "sync/index.ts",
                "--server",
                host.url,
                "--dir",
                mirror,
                "--folders",
                "docs",
                "--write",
                "docs",
                "--token",
                "wrong-token",
              ],
              {
                cwd: `${import.meta.dirname}/../..`,
                env: { FOLDER_SYNC_WRITER_TOKEN: "correct-token" },
              },
            ),
          );

          const output = yield* cli.all.pipe(
            Stream.decodeText,
            Stream.runCollect,
            Effect.forkScoped,
          );

          expect(yield* cli.exitCode).toBe(ChildProcessSpawner.ExitCode(1));
          const messages = (yield* Fiber.join(output)).join("");
          expect(messages).toContain("Writer authorization failed");
          expect(messages).toContain("Pending writes remain queued");
          expect(messages).not.toContain("wrong-token");
          const context = yield* Layer.build(stateLayer(`${mirror}/.folder-sync/state.sqlite`));
          const state = yield* SyncState.pipe(Effect.provideContext(context));
          const pending = yield* state.pending();
          expect(pending).toHaveLength(1);
          expect(pending[0]?.path).toBe("local.md");
          expect(new TextDecoder().decode(pending[0]?.bytes ?? new Uint8Array())).toBe(
            "pending local bytes",
          );
          expect(yield* state.rejected()).toEqual([]);
          const history = yield* HttpClient.get(`${host.url}/api/folders/docs/history/local.md`);
          expect(yield* history.json).toEqual([]);

          const restarted = yield* spawner.spawn(
            ChildProcess.make(
              "bun",
              [
                "sync/index.ts",
                "--server",
                host.url,
                "--dir",
                mirror,
                "--folders",
                "docs",
                "--write",
                "docs",
              ],
              {
                cwd: `${import.meta.dirname}/../..`,
                env: { FOLDER_SYNC_WRITER_TOKEN: "correct-token" },
              },
            ),
          );

          const restartedOutput = yield* restarted.all.pipe(Stream.runDrain, Effect.forkScoped);
          yield* until(state.pending(), (rows) => rows.length === 0);
          const accepted = yield* HttpClient.get(`${host.url}/api/folders/docs/history/local.md`);
          expect(yield* accepted.json).toHaveLength(1);
          yield* restarted.kill({ killSignal: "SIGTERM" });
          expect(yield* restarted.exitCode).toBe(ChildProcessSpawner.ExitCode(130));
          yield* Fiber.join(restartedOutput);
        }),
      { timeout: 15000 },
    );
  },
);
