import { BunCrypto, BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { spawnServer } from "@streamsy/test-support/server";
import { Deferred, Effect, Fiber, FileSystem, Layer, Schedule, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { FileOp, Version } from "../../server/domain.ts";
import { encodeFilePath } from "../../sync/decisions.ts";
import { runSync, sync } from "../../sync/engine.ts";
import { RemoteFailure, SyncRemote, remoteLayer } from "../../sync/client.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";

class Waiting extends Schema.TaggedError<Waiting>()("SyncTestWaiting", {}) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 600 })),
    }),
  );

const text = (value: string) => new TextEncoder().encode(value);

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

const meta = (opId: string, base?: string) => {
  const common = { "x-file-op-id": opId, "x-file-author": "test", "x-file-origin": "test" };

  return base === undefined ? common : { ...common, "x-file-base": base };
};

const fileUrl = (server: string, folder: string, file: string) =>
  `${server}/api/folders/${encodeURIComponent(folder)}/files/${encodeFilePath(file)}`;

const put = Effect.fn("SyncTest.put")(function* (
  server: string,
  folder: string,
  file: string,
  value: string,
  opId: string,
) {
  const response = yield* HttpClient.put(fileUrl(server, folder, file), {
    headers: meta(opId),
    body: HttpBody.uint8Array(text(value)),
  });

  expect(response.status).toBe(200);

  return yield* Schema.decodeUnknownEffect(Written)(yield* response.json);
});

const history = Effect.fn("SyncTest.history")(function* (
  server: string,
  folder: string,
  file: string,
) {
  const response = yield* HttpClient.get(
    `${server}/api/folders/${encodeURIComponent(folder)}/history/${encodeFilePath(file)}`,
  );

  return yield* Schema.decodeUnknownEffect(History)(yield* response.json);
});

const create = Effect.fn("SyncTest.create")(function* (server: string, folderId: string) {
  const response = yield* HttpClient.post(`${server}/api/folders`, {
    body: yield* HttpBody.json({ folderId, title: folderId }),
  });

  expect(response.status).toBe(200);
  yield* response.text;
});

const inspect = Effect.fn("SyncTest.inspect")(function* (directory: string) {
  const context = yield* Layer.build(stateLayer(`${directory}/.folder-sync/state.sqlite`));

  return yield* SyncState.pipe(Effect.provideContext(context));
});

layer(Layer.mergeAll(BunServices.layer, BunCrypto.layer, FetchHttpClient.layer), {
  excludeTestServices: true,
})((it) => {
  it.effect(
    "two mirrors converge across offline replay, CLI restart, deletes, renames and read-only edits",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-integration-");
        const fs = yield* FileSystem.FileSystem;
        const a = `${directory}/a`;
        const b = `${directory}/b`;
        const database = `${directory}/server.sqlite`;
        const cwd = `${import.meta.dir}/../..`;

        const server = yield* spawnServer({
          cwd,
          entry: "server/index.ts",
          env: { FOLDER_SYNC_DB: database },
        });

        const folder = "daily/demo";
        yield* create(server.url, folder);
        yield* create(server.url, "ro");
        yield* create(server.url, "excluded");
        const initial = yield* put(server.url, folder, "seed.md", "seed", "seed");
        yield* put(server.url, "ro", "read.md", "read only", "readonly");
        yield* put(server.url, "excluded", "secret.md", "excluded", "excluded");

        const options = (mirror: string) => ({
          server: server.url,
          directory: mirror,
          folders: [folder, "ro"],
          writable: [folder],
          settleMillis: 60,
        });

        let clientA = yield* sync(options(a)).pipe(Effect.forkScoped);
        const clientB = yield* sync(options(b)).pipe(Effect.forkScoped);

        const read = (absolute: string) =>
          fs.readFileString(absolute).pipe(Effect.orElseSucceed(() => "<missing>"));

        yield* until(read(`${a}/${folder}/seed.md`), (value) => value === "seed");
        yield* until(read(`${b}/${folder}/seed.md`), (value) => value === "seed");
        const stateA = yield* inspect(a);
        const stateB = yield* inspect(b);
        yield* until(stateA.get("ready"), (value) => value === "true");
        yield* until(stateB.get("ready"), (value) => value === "true");
        expect(yield* fs.exists(`${a}/excluded`)).toBe(false);
        expect(yield* fs.exists(`${b}/excluded`)).toBe(false);

        // Observe three completed scanner passes. A pulled file must never append back.
        for (const state of [stateA, stateB]) {
          for (let pass = 0; pass < 3; pass++) {
            const before = yield* state.get("lastScan");
            yield* until(state.get("lastScan"), (value) => value !== before);
          }

          expect(yield* state.pending()).toEqual([]);
        }

        expect(yield* history(server.url, folder, "seed.md")).toHaveLength(1);
        expect((yield* stateA.head(folder, "seed.md"))?.version).toBe(initial.version);

        yield* fs.writeFileString(`${a}/${folder}/a.md`, "from A");
        yield* fs.writeFileString(`${b}/${folder}/b.md`, "from B");
        yield* until(read(`${b}/${folder}/a.md`), (value) => value === "from A");
        yield* until(read(`${a}/${folder}/b.md`), (value) => value === "from B");

        yield* fs.writeFileString(`${a}/${folder}/.editor-tmp`, "renamed content");
        yield* fs.rename(`${a}/${folder}/.editor-tmp`, `${a}/${folder}/雪 100%.md`);
        yield* until(read(`${b}/${folder}/雪 100%.md`), (value) => value === "renamed content");
        expect(yield* history(server.url, folder, ".editor-tmp")).toEqual([]);
        expect(yield* history(server.url, folder, "雪 100%.md")).toHaveLength(1);
        // Only the mirror's own root state directory is ignored, not similarly named user files.
        yield* fs.makeDirectory(`${a}/${folder}/.folder-sync`, { recursive: true });
        yield* fs.writeFileString(`${a}/${folder}/.folder-sync/user.md`, "user hidden file");
        yield* until(
          read(`${b}/${folder}/.folder-sync/user.md`),
          (value) => value === "user hidden file",
        );

        yield* fs.remove(`${a}/${folder}/seed.md`);
        yield* until(read(`${b}/${folder}/seed.md`), (value) => value === "<missing>");
        const deleted = yield* until(stateB.head(folder, "seed.md"), (row) => row?.hash === null);
        expect(deleted?.version).not.toBeNull();
        yield* fs.writeFileString(`${b}/${folder}/seed.md`, "recreated");
        yield* until(read(`${a}/${folder}/seed.md`), (value) => value === "recreated");
        const recreated = (yield* history(server.url, folder, "seed.md")).at(-1);
        expect(recreated?.operation.base).toBe(deleted?.version ?? null);
        expect(recreated?.operation.overwrote).toBeUndefined();

        yield* fs.writeFileString(`${a}/ro/read.md`, "must restore");
        yield* until(read(`${a}/ro/read.md`), (value) => value === "read only");
        expect(yield* history(server.url, "ro", "read.md")).toHaveLength(1);
        yield* fs.writeFileString(`${a}/ro/extra.md`, "remove local extra");
        yield* until(fs.exists(`${a}/ro/extra.md`), (value) => !value);

        yield* server.stop;
        yield* fs.writeFileString(`${a}/${folder}/offline-a.md`, "offline A");
        yield* fs.writeFileString(`${b}/${folder}/offline-b.md`, "offline B");
        yield* fs.writeFileString(`${a}/${folder}/shared.md`, "offline shared A");
        yield* fs.writeFileString(`${b}/${folder}/shared.md`, "offline shared B");
        yield* until(
          stateA.pending(),
          (rows) =>
            rows.some((row) => row.path === "offline-a.md") &&
            rows.some((row) => row.path === "shared.md"),
        );
        yield* until(
          stateB.pending(),
          (rows) =>
            rows.some((row) => row.path === "offline-b.md") &&
            rows.some((row) => row.path === "shared.md"),
        );
        const savedIds = (yield* stateA.pending()).map((row) => row.opId);
        yield* Fiber.interrupt(clientA);
        clientA = yield* sync(options(a)).pipe(Effect.forkScoped);
        const scanBeforeRestart = yield* stateA.get("lastScan");
        yield* until(stateA.get("lastScan"), (value) => value !== scanBeforeRestart);
        expect((yield* stateA.pending()).map((row) => row.opId)).toEqual(savedIds);

        const restart = yield* spawnServer({
          cwd,
          entry: "server/index.ts",
          port: server.port,
          env: { FOLDER_SYNC_DB: database },
        });

        yield* until(read(`${b}/${folder}/offline-a.md`), (value) => value === "offline A");
        yield* until(read(`${a}/${folder}/offline-b.md`), (value) => value === "offline B");
        yield* until(stateA.pending(), (rows) => rows.length === 0);
        yield* until(stateB.pending(), (rows) => rows.length === 0);
        const serverShared = yield* HttpClient.get(fileUrl(restart.url, folder, "shared.md"));
        const winningBytes = yield* serverShared.text;
        yield* until(read(`${a}/${folder}/shared.md`), (value) => value === winningBytes);
        yield* until(read(`${b}/${folder}/shared.md`), (value) => value === winningBytes);
        expect(yield* history(restart.url, folder, "shared.md")).toHaveLength(2);
        expect(yield* history(restart.url, folder, "offline-a.md")).toHaveLength(1);
        expect(yield* history(restart.url, folder, "offline-b.md")).toHaveLength(1);
        yield* Fiber.interrupt(clientA);
        yield* Fiber.interrupt(clientB);
        yield* restart.stop;
      }),
    { timeout: 40000 },
  );
  it.effect(
    "lost responses and 503 reads replay the same opId, preserve intervening writes, and reset the feed",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-replay-");
        const fs = yield* FileSystem.FileSystem;
        const mirror = `${directory}/mirror`;
        yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        yield* put(server.url, "docs", "a.md", "original", "original");

        const context = yield* Layer.build(
          Layer.merge(
            stateLayer(`${mirror}/.folder-sync/state.sqlite`),
            remoteLayer(server.url).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const state = yield* SyncState.pipe(Effect.provideContext(context));
        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        yield* until(remote.snapshot, (snapshot) => snapshot.files.length === 1);
        const lost = yield* Deferred.make<void>();
        const releaseRetry = yield* Deferred.make<void>();
        const attempts: string[] = [];
        let snapshots = 0;
        let firstTail = true;
        let failHead = true;
        const http = yield* HttpClient.HttpClient;

        const intercepted = SyncRemote.of({
          ...remote,
          snapshot: remote.snapshot.pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                snapshots++;
              }),
            ),
          ),
          tail: Effect.suspend(() => {
            if (firstTail) {
              firstTail = false;

              return Effect.succeed("0000000000000000_0000000000000000");
            }

            return remote.tail;
          }),
          push: (operation, origin) =>
            Effect.gen(function* () {
              attempts.push(operation.opId);

              if (attempts.length > 1) yield* Deferred.await(releaseRetry);
              const receipt = yield* remote.push(operation, origin);

              if (attempts.length === 1) {
                // The server accepted A, then B changed the head before A learned its receipt.
                yield* put(server.url, "docs", "a.md", "from B", "external-B").pipe(
                  Effect.provideService(HttpClient.HttpClient, http),
                );
                yield* Deferred.succeed(lost, undefined);

                return yield* new RemoteFailure({
                  message: "response lost after commit",
                  status: 503,
                  retryable: true,
                });
              }

              return receipt;
            }),
          head: (folder, file, minVersion) =>
            Effect.suspend(() => {
              if (minVersion !== undefined && failHead) {
                failHead = false;

                return Effect.fail(
                  new RemoteFailure({
                    message: "minVersion temporarily unavailable",
                    status: 503,
                    retryable: true,
                  }),
                );
              }

              return remote.head(folder, file, minVersion);
            }),
        });

        const daemon = yield* runSync({
          server: server.url,
          directory: mirror,
          folders: ["docs"],
          writable: ["docs"],
          settleMillis: 60,
        }).pipe(
          Effect.provideService(SyncRemote, intercepted),
          Effect.provideContext(context),
          Effect.forkScoped,
        );

        yield* until(state.get("ready"), (value) => value === "true");
        yield* until(
          Effect.sync(() => snapshots),
          (value) => value === 2,
        );
        yield* until(state.get("ready"), (value) => value === "true");
        yield* fs.writeFileString(`${mirror}/docs/a.md`, "from A");
        yield* Deferred.await(lost);
        const pending = yield* state.pending();
        expect(pending).toHaveLength(1);
        expect(pending[0]?.opId).toBe(attempts[0]);
        yield* Deferred.succeed(releaseRetry, undefined);
        yield* until(state.pending(), (rows) => rows.length === 0);
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("from B");
        expect(attempts.length).toBeGreaterThanOrEqual(3);
        expect(new Set(attempts).size).toBe(1);
        expect(
          (yield* history(server.url, "docs", "a.md")).map((row) =>
            row.operation.type === "put" && "inline" in row.operation ? row.operation.inline : "",
          ),
        ).toEqual(["original", "from A", "from B"]);
        const before = yield* state.get("lastScan");
        yield* until(state.get("lastScan"), (value) => value !== before);
        expect(yield* state.pending()).toEqual([]);
        expect(yield* history(server.url, "docs", "a.md")).toHaveLength(3);
        yield* Fiber.interrupt(daemon);
        yield* server.stop;
      }),
    { timeout: 20000 },
  );
  it.effect(
    "a remote write after the delivery head read still reaches the mirror",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-delivery-race-");
        const mirror = `${directory}/mirror`;
        const fs = yield* FileSystem.FileSystem;
        const http = yield* HttpClient.HttpClient;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        yield* put(server.url, "docs", "a.md", "seed", "seed");
        yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

        const context = yield* Layer.build(
          Layer.merge(
            stateLayer(`${mirror}/.folder-sync/state.sqlite`),
            remoteLayer(server.url).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        const state = yield* SyncState.pipe(Effect.provideContext(context));
        let delivered: string | null = null;
        let external: string | undefined;

        const wrapped = SyncRemote.of({
          ...remote,
          push: (op, origin) =>
            remote.push(op, origin).pipe(
              Effect.tap((receipt) =>
                Effect.sync(() => {
                  delivered = receipt.version;
                }),
              ),
            ),
          head: (folder, file, minVersion) =>
            Effect.gen(function* () {
              const head = yield* remote.head(folder, file, minVersion);

              if (delivered !== null && minVersion === delivered) {
                delivered = null;

                const receipt = yield* put(server.url, "docs", "a.md", "from B", "B").pipe(
                  Effect.provideService(HttpClient.HttpClient, http),
                );

                external = receipt.version;
                // Let the feed fetch B while delivery still owns its outbox entry.
                yield* Effect.yieldNow;
              }

              return head;
            }),
        });

        const daemon = yield* runSync({
          server: server.url,
          directory: mirror,
          folders: ["docs"],
          writable: ["docs"],
        }).pipe(
          Effect.provideService(SyncRemote, wrapped),
          Effect.provideContext(context),
          Effect.forkScoped,
        );

        yield* until(state.get("ready"), (value) => value === "true");
        yield* fs.writeFileString(`${mirror}/docs/a.md`, "from A");
        yield* until(fs.readFileString(`${mirror}/docs/a.md`), (value) => value === "from B");
        yield* until(state.pending(), (rows) => rows.length === 0);
        expect((yield* state.head("docs", "a.md"))?.version).toBe(external);
        yield* Fiber.interrupt(daemon);
        yield* server.stop;
      }),
    { timeout: 20000 },
  );
  it.effect(
    "the CLI executable accepts flags and the runner supports custom folder mappings",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-cli-");
        const fs = yield* FileSystem.FileSystem;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "daily/demo");
        yield* put(server.url, "daily/demo", "a.md", "CLI seed", "seed");
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

        const cli = yield* spawner.spawn(
          ChildProcess.make(
            "bun",
            [
              "sync/index.ts",
              "--server",
              server.url,
              "--dir",
              `${directory}/cli`,
              "--folders",
              "daily/demo",
              "--write",
              "daily/demo",
            ],
            { cwd: `${import.meta.dir}/../..` },
          ),
        );

        const read = (absolute: string) =>
          fs.readFileString(absolute).pipe(Effect.orElseSucceed(() => "<missing>"));

        yield* until(read(`${directory}/cli/daily/demo/a.md`), (value) => value === "CLI seed");
        yield* fs.writeFileString(`${directory}/cli/daily/demo/new.md`, "CLI write");
        yield* until(history(server.url, "daily/demo", "new.md"), (rows) => rows.length === 1);

        const mapped = yield* sync({
          server: server.url,
          directory: `${directory}/mapped`,
          folders: ["daily/demo"],
          writable: [],
          folderDirectory: () => "docs",
        }).pipe(Effect.forkScoped);

        yield* until(read(`${directory}/mapped/docs/new.md`), (value) => value === "CLI write");
        expect(yield* fs.exists(`${directory}/mapped/daily`)).toBe(false);
        yield* Fiber.interrupt(mapped);
        yield* cli.kill({ killSignal: "SIGTERM" });
        expect(yield* cli.exitCode).toBe(ChildProcessSpawner.ExitCode(130));
        yield* server.stop;
      }),
    { timeout: 15000 },
  );
  it.effect(
    "an interrupted pull finishes on restart without pushing an echo",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-interrupted-pull-");
        const fs = yield* FileSystem.FileSystem;
        const mirror = `${directory}/mirror`;
        yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        const original = yield* put(server.url, "docs", "a.md", "original", "original");

        const context = yield* Layer.build(
          Layer.merge(
            stateLayer(`${mirror}/.folder-sync/state.sqlite`),
            remoteLayer(server.url).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const state = yield* SyncState.pipe(Effect.provideContext(context));
        const interrupted = yield* Deferred.make<void>();
        let blockRename = false;

        const wrapped = FileSystem.FileSystem.of({
          ...fs,
          rename: (from, to) =>
            blockRename && to === `${mirror}/docs/a.md`
              ? Deferred.succeed(interrupted, undefined).pipe(Effect.andThen(Effect.never))
              : fs.rename(from, to),
        });

        const options = {
          server: server.url,
          directory: mirror,
          folders: ["docs"],
          writable: ["docs"],
        };

        const daemon = yield* runSync(options).pipe(
          Effect.provideService(FileSystem.FileSystem, wrapped),
          Effect.provideContext(context),
          Effect.forkScoped,
        );

        yield* until(state.get("ready"), (value) => value === "true");
        yield* until(state.head("docs", "a.md"), (head) => head?.localVersion === original.version);
        blockRename = true;
        const replacement = yield* put(server.url, "docs", "a.md", "replacement", "replacement");
        yield* Deferred.await(interrupted);
        yield* Fiber.interrupt(daemon);
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("original");
        const unfinished = yield* state.head("docs", "a.md");
        expect(unfinished?.version).toBe(replacement.version);
        expect(unfinished?.localVersion).toBe(original.version);

        const restarted = yield* runSync(options).pipe(
          Effect.provideContext(context),
          Effect.forkScoped,
        );

        yield* until(
          state.head("docs", "a.md"),
          (head) => head?.localVersion === replacement.version,
        );
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("replacement");
        const before = yield* state.get("lastScan");
        yield* until(state.get("lastScan"), (value) => value !== before);
        expect(yield* state.pending()).toEqual([]);
        expect(yield* history(server.url, "docs", "a.md")).toHaveLength(2);
        yield* Fiber.interrupt(restarted);
        yield* server.stop;
      }),
    { timeout: 15000 },
  );
});
