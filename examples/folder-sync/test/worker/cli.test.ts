import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Fiber, FileSystem, Layer, Schedule, Schema, Stream } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";

class Waiting extends Schema.TaggedError<Waiting, { readonly brand: unique symbol }>()(
  "WorkerCliWaiting",
  {},
) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 300 })),
    }),
  );

const headers = { authorization: "Bearer correct-token" };

const digest = (text: string) => new Bun.CryptoHasher("sha256").update(text).digest("hex");

const createFolder = (url: string, folderId: string) =>
  HttpClient.post(`${url}/api/folders`, {
    headers,
    body: HttpBody.jsonUnsafe({ txid: `create:${folderId}`, folderId, title: folderId }),
  }).pipe(
    Effect.tap((response) => Effect.sync(() => expect(response.status, folderId).toBe(200))),
    Effect.andThen((response) => response.text),
  );

/** Upload the text, then commit it as the first version of `a.md`. */
const writeFirst = (url: string, folderId: string, text: string) =>
  HttpClient.put(`${url}/api/blobs/${digest(text)}`, {
    headers,
    body: HttpBody.text(text, "application/octet-stream"),
  }).pipe(
    Effect.andThen((response) => response.text),
    Effect.andThen(
      HttpClient.put(`${url}/api/folders/${folderId}/files/a.md`, {
        headers,
        body: HttpBody.jsonUnsafe({
          txid: `write:${folderId}`,
          parent: null,
          author: "test",
          hash: digest(text),
          size: new TextEncoder().encode(text).length,
        }),
      }),
    ),
    Effect.tap((response) => Effect.sync(() => expect(response.status, folderId).toBe(200))),
    Effect.andThen((response) => response.text),
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
            body: yield* HttpBody.json({ txid: "create:docs", folderId: "docs", title: "Docs" }),
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
          const messages = Array.join(yield* Fiber.join(output), "");
          expect(messages).toContain("Server answered 401");
          expect(messages).not.toContain("wrong-token");
          const context = yield* Layer.build(stateLayer(`${mirror}/.folder-sync/state.sqlite`));
          const state = yield* SyncState.pipe(Effect.provideContext(context));
          const pending = yield* state.pending();
          expect(pending).toHaveLength(1);
          expect(pending[0]?.path).toBe("local.md");
          const source = pending[0]?.source;

          if (source === undefined || source === null)
            return yield* Effect.die("Missing retained source");
          expect(yield* fs.readFileString(source)).toBe("pending local bytes");
          expect(yield* state.rejected()).toEqual([]);

          const initial = yield* HttpClient.get(
            `${host.url}/streams/folders/646f6373/files?offset=-1`,
          );

          expect(yield* initial.json).toEqual([]);

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

          const accepted = yield* HttpClient.get(
            `${host.url}/streams/folders/646f6373/files?offset=-1`,
          );

          expect(yield* accepted.json).toHaveLength(1);
          const read = yield* HttpClient.get(`${host.url}/api/folders/docs/files/local.md`);
          expect(yield* read.text).toBe("pending local bytes");
          yield* restarted.kill({ killSignal: "SIGTERM" });
          expect(yield* restarted.exitCode).toBe(ChildProcessSpawner.ExitCode(130));
          yield* Fiber.join(restartedOutput);

          return yield* Effect.void;
        }),
      { timeout: 15000 },
    );

    it.effect(
      "a CLI that follows the registry keeps running when a folder is created",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-cli-create-");
          const fs = yield* FileSystem.FileSystem;
          const mirror = `${directory}/mirror`;

          const host = yield* Effect.acquireRelease(
            Effect.promise(() => startWorker(`${directory}/worker`, "correct-token", 0, true)),
            (runtime) => Effect.promise(runtime.stop),
          );

          yield* createFolder(host.url, "first");
          yield* writeFirst(host.url, "first", "first bytes");
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

          // No --folders: the CLI mirrors every folder that the registry lists.
          const cli = yield* spawner.spawn(
            ChildProcess.make("bun", ["sync/index.ts", "--server", host.url, "--dir", mirror], {
              cwd: `${import.meta.dirname}/../..`,
            }),
          );

          const output = yield* cli.all.pipe(
            Stream.decodeText,
            Stream.runCollect,
            Effect.forkScoped,
          );

          const mirrored = (path: string) =>
            fs.readFileString(`${mirror}/${path}`).pipe(Effect.orElseSucceed(() => ""));

          yield* until(mirrored("first/a.md"), (text) => text === "first bytes");

          // Widen the gap between the two steps of the create, as on Cloudflare.
          const configured = yield* HttpClient.post(`${host.url}/api/admin/test-folder`, {
            headers,
            body: HttpBody.jsonUnsafe({ folderId: "late", createDelay: 1000 }),
          });

          expect(configured.status).toBe(200);
          yield* configured.text;
          yield* createFolder(host.url, "late");
          yield* writeFirst(host.url, "late", "late bytes");
          yield* until(mirrored("late/a.md"), (text) => text === "late bytes");

          yield* cli.kill({ killSignal: "SIGTERM" });
          const exitCode = yield* cli.exitCode;
          const messages = Array.join(yield* Fiber.join(output), "");
          expect(messages).not.toContain("Server answered 404");
          expect(exitCode).toBe(ChildProcessSpawner.ExitCode(130));

          return yield* Effect.void;
        }),
      { timeout: 20000 },
    );
  },
);
