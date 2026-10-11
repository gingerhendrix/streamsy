import { BunCrypto, BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { spawnServer, token } from "./host.ts";
import { Deferred, Effect, Fiber, FileSystem, Layer, Schedule, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { Offset } from "@streamsy/core";
import { FILE_LIMIT, FileOp, Version } from "../../server/domain.ts";
import { conflictPath, encodeFilePath } from "../../sync/decisions.ts";
import { runSync, sync as startSync } from "../../sync/engine.ts";
import { RemoteFailure, SyncRemote, remoteLayer } from "../../sync/client.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";

const sync = (options: Parameters<typeof startSync>[0]) => startSync({ ...options, token });

const clientLayer = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(HttpClient.HttpClient, (client) =>
    HttpClient.mapRequest(client, HttpClientRequest.bearerToken(token)),
  ),
).pipe(Layer.provide(FetchHttpClient.layer));

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

layer(Layer.mergeAll(BunServices.layer, BunCrypto.layer, clientLayer), {
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
        expect(yield* fs.readFileString(`${a}/.folder-sync/rejected/ro/read.md`)).toBe(
          "must restore",
        );
        yield* fs.writeFileString(`${a}/ro/read.md`, "second restore");
        yield* until(read(`${a}/ro/read.md`), (value) => value === "read only");
        expect(yield* fs.readFileString(`${a}/.folder-sync/rejected/ro/read-1.md`)).toBe(
          "second restore",
        );
        yield* fs.writeFileString(`${a}/ro/extra.md`, "remove local extra");
        yield* until(fs.exists(`${a}/ro/extra.md`), (value) => !value);
        expect(yield* fs.readFileString(`${a}/.folder-sync/rejected/ro/extra.md`)).toBe(
          "remove local extra",
        );

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
        const sharedHistory = yield* history(restart.url, folder, "shared.md");
        expect(sharedHistory).toHaveLength(2);
        expect(sharedHistory[0]?.operation.overwrote).toBeUndefined();
        expect(sharedHistory[1]?.operation.overwrote).toBe(sharedHistory[0]?.version);
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
            remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
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

              return remote.tail.pipe(
                Effect.map((head) => ({ ...head, cursor: "0000000000000000_0000000000000000" })),
              );
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
        const replayHistory = yield* history(server.url, "docs", "a.md");
        expect(replayHistory).toHaveLength(3);
        expect(replayHistory[0]?.operation.overwrote).toBeUndefined();
        expect(replayHistory[1]?.operation.overwrote).toBeUndefined();
        expect(replayHistory[2]?.operation.overwrote).toBe(replayHistory[1]?.version);
        expect(replayHistory.filter((row) => row.operation.opId === attempts[0])).toHaveLength(1);
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
            remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        const state = yield* SyncState.pipe(Effect.provideContext(context));
        let delivered: number | null = null;
        let external: number | undefined;
        const fetched = yield* Deferred.make<void>();

        const wrapped = SyncRemote.of({
          ...remote,
          push: (op, origin) =>
            Effect.gen(function* () {
              const receipt = yield* remote.push(op, origin);
              delivered = receipt.version;
              // Ensure A's feed page settles while its outbox entry is still pending.
              yield* until(remote.snapshot, (snapshot) =>
                snapshot.files.some((row) => row.version === receipt.version),
              ).pipe(Effect.orDie);
              const target = yield* remote.tail;
              yield* until(
                state.get("cursor"),
                (cursor) => cursor !== undefined && cursor >= target.cursor,
              ).pipe(Effect.orDie);

              return receipt;
            }),
          page: (cursor, live) =>
            remote.page(cursor, live).pipe(
              Effect.tap((page) =>
                Effect.gen(function* () {
                  if (page.rows.some((row) => row.version === external))
                    yield* Deferred.succeed(fetched, undefined);
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
                // B is fetched while delivery retains A's entry. Under the old lock order
                // the feed could skip B; with the gate held it must wait for A to settle.
                yield* Deferred.await(fetched);
                yield* until(remote.snapshot, (snapshot) =>
                  snapshot.files.some((row) => row.version === external),
                ).pipe(Effect.orDie);
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
        yield* until(
          fs.readFileString(`${mirror}/docs/a.md`).pipe(Effect.orElseSucceed(() => "<missing>")),
          (value) => value === "seed",
        );
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
    "fresh bootstrap adopts identical files and preserves stale bytes as conflict siblings",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-existing-");
        const mirror = `${directory}/mirror`;
        const fs = yield* FileSystem.FileSystem;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        const same = yield* put(server.url, "docs", "same.md", "identical", "same");
        const stale = yield* put(server.url, "docs", "stale.md", "new on server", "stale");
        yield* fs.makeDirectory(`${mirror}/docs`, { recursive: true });
        yield* fs.writeFileString(`${mirror}/docs/same.md`, "identical");
        yield* fs.writeFileString(`${mirror}/docs/stale.md`, "old local copy");
        yield* fs.writeFileString(`${mirror}/docs/new.md`, "local new file");

        const daemon = yield* sync({
          server: server.url,
          directory: mirror,
          folders: ["docs"],
          writable: ["docs"],
        }).pipe(Effect.forkScoped);

        yield* until(
          fs.readFileString(`${mirror}/docs/stale.md`),
          (value) => value === "new on server",
        );
        const state = yield* inspect(mirror);
        const origin = yield* state.get("origin");
        const copy = `stale.conflict-${origin}.md`;
        yield* until(history(server.url, "docs", copy), (rows) => rows.length === 1);
        yield* until(history(server.url, "docs", "new.md"), (rows) => rows.length === 1);
        expect(yield* fs.readFileString(`${mirror}/docs/${copy}`)).toBe("old local copy");
        expect((yield* state.head("docs", "same.md"))?.version).toBe(same.version);
        expect(yield* history(server.url, "docs", "same.md")).toHaveLength(1);
        expect(yield* history(server.url, "docs", "stale.md")).toEqual([
          expect.objectContaining({
            version: stale.version,
            operation: expect.objectContaining({ inline: "new on server" }),
          }),
        ]);
        yield* Fiber.interrupt(daemon);
        yield* server.stop;
      }),
    { timeout: 20000 },
  );
  it.effect(
    "two offline saves rebase onto their own receipts without unseen overwrite marks",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-own-saves-");
        const mirror = `${directory}/mirror`;
        const fs = yield* FileSystem.FileSystem;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        const seed = yield* put(server.url, "docs", "a.md", "seed", "seed");
        yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

        const context = yield* Layer.build(
          Layer.merge(
            stateLayer(`${mirror}/.folder-sync/state.sqlite`),
            remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        const state = yield* SyncState.pipe(Effect.provideContext(context));
        const reconnect = yield* Deferred.make<void>();
        let offline = true;

        const wrapped = SyncRemote.of({
          ...remote,
          push: (op, origin) =>
            Effect.gen(function* () {
              if (offline) yield* Deferred.await(reconnect);

              return yield* remote.push(op, origin);
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
        yield* until(state.head("docs", "a.md"), (head) => head?.localVersion === seed.version);
        yield* fs.writeFileString(`${mirror}/docs/a.md`, "save 1");
        yield* until(state.pending(), (rows) => rows.length === 1);
        yield* fs.writeFileString(`${mirror}/docs/a.md`, "save 2");
        const queued = yield* until(state.pending(), (rows) => rows.length === 2);
        expect(queued.map((row) => row.base)).toEqual([seed.version, seed.version]);
        offline = false;
        yield* Deferred.succeed(reconnect, undefined);
        yield* until(state.pending(), (rows) => rows.length === 0);
        const rows = yield* history(server.url, "docs", "a.md");
        expect(rows).toHaveLength(3);
        expect(rows[1]?.operation.base).toBe(seed.version);
        expect(rows[2]?.operation.base).toBe(rows[1]?.version);
        expect(rows.every((row) => row.operation.overwrote === undefined)).toBe(true);
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("save 2");
        yield* Fiber.interrupt(daemon);
        yield* server.stop;
      }),
    { timeout: 20000 },
  );
  it.effect(
    "oversize files, symlinks and rejected writes leave other files syncing",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-skips-");
        const mirror = `${directory}/mirror`;
        const fs = yield* FileSystem.FileSystem;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");

        const daemon = yield* sync({
          server: server.url,
          directory: mirror,
          folders: ["docs", "missing"],
          writable: ["docs", "missing"],
        }).pipe(Effect.forkScoped);

        yield* until(fs.exists(`${mirror}/.folder-sync/state.sqlite`), Boolean);
        const state = yield* inspect(mirror);
        yield* until(state.get("ready"), (value) => value === "true");
        yield* fs.writeFile(`${mirror}/docs/large.bin`, new Uint8Array(FILE_LIMIT + 1));
        yield* fs.writeFileString(`${directory}/outside.md`, "outside");
        yield* fs.symlink(`${directory}/outside.md`, `${mirror}/docs/link.md`);
        yield* fs.symlink(`${directory}/absent.md`, `${mirror}/docs/broken.md`);
        yield* fs.writeFileString(`${mirror}/missing/reject.md`, "retained rejected bytes");
        const rejected = yield* until(state.rejected(), (rows) => rows.length === 1);
        expect(rejected[0]?.status).toBe(404);
        expect(new TextDecoder().decode(rejected[0]?.bytes ?? new Uint8Array())).toBe(
          "retained rejected bytes",
        );
        yield* fs.writeFileString(`${mirror}/docs/good.md`, "still syncing");
        yield* until(history(server.url, "docs", "good.md"), (rows) => rows.length === 1);
        yield* until(state.pending(), (rows) => rows.length === 0);

        for (const file of ["large.bin", "link.md", "broken.md"])
          expect(yield* history(server.url, "docs", file)).toHaveLength(0);
        expect(yield* fs.readFileString(`${directory}/outside.md`)).toBe("outside");
        const scan = yield* state.get("lastScan");
        yield* until(state.get("lastScan"), (value) => value !== scan);
        expect(yield* state.rejected()).toHaveLength(1);
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

        const listing = yield* spawner.string(
          ChildProcess.make("bun", ["sync/index.ts", "rejected", "--dir", mirror], {
            cwd: `${import.meta.dir}/../..`,
          }),
        );

        expect(listing).toContain('"status":404');
        expect(listing).toContain('"path":"reject.md"');
        expect(listing).not.toContain("retained rejected bytes");
        expect(yield* state.rejected()).toHaveLength(1);
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
              "--token",
              token,
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
            remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
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
  it.effect(
    "a new feed epoch bootstraps a replacement server even when its cursor and versions equal the old feed",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-epoch-");
        const mirror = `${directory}/mirror`;
        const fs = yield* FileSystem.FileSystem;
        yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
          env: { FOLDER_SYNC_DB: `${directory}/old.sqlite` },
        });

        yield* create(server.url, "docs");
        const initial = yield* put(server.url, "docs", "a.md", "old index", "old");

        const context = yield* Layer.build(
          Layer.merge(
            stateLayer(`${mirror}/.folder-sync/state.sqlite`),
            remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
          ),
        );

        const state = yield* SyncState.pipe(Effect.provideContext(context));
        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));

        yield* until(remote.snapshot, (snapshot) =>
          snapshot.files.some((row) => row.version === initial.version),
        );

        const blocked = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        let pauseReads = false;
        let bootstraps = 0;

        const read = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            if (pauseReads) {
              yield* Deferred.succeed(blocked, undefined);
              yield* Deferred.await(resume);
            }

            return yield* effect;
          });

        const daemon = yield* runSync({
          server: server.url,
          directory: mirror,
          folders: ["docs"],
          writable: [],
        }).pipe(
          Effect.provideService(SyncRemote, {
            ...remote,
            snapshot: read(remote.snapshot).pipe(Effect.tap(() => Effect.sync(() => bootstraps++))),
            tail: read(remote.tail),
            head: (...args) => read(remote.head(...args)),
            page: (cursor) => read(remote.page(cursor, false)),
          }),
          Effect.provideContext(context),
          Effect.forkScoped,
        );

        yield* until(state.get("ready"), (value) => value === "true");

        const oldEpoch = yield* Schema.decodeUnknownEffect(Schema.String)(
          yield* state.get("epoch"),
        );

        const oldCursor = yield* Schema.decodeUnknownEffect(Offset)(yield* state.get("cursor"));
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("old index");
        expect(bootstraps).toBe(1);

        // Stop at a read boundary before switching servers. The replacement must expose
        // equal cursors and versions before the daemon can observe its different epoch.
        pauseReads = true;
        yield* Deferred.await(blocked);
        yield* server.stop;

        const replacement = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
          port: server.port,
          env: { FOLDER_SYNC_DB: `${directory}/new.sqlite` },
        });

        yield* create(replacement.url, "docs");
        const replaced = yield* put(replacement.url, "docs", "a.md", "replacement index", "new");
        expect(replaced.version).toBe(initial.version);

        const snapshot = yield* until(remote.snapshot, (value) =>
          value.files.some((row) => row.version === replaced.version),
        );

        expect(snapshot.cursor).toBe(oldCursor);
        expect(snapshot.epoch).not.toBe(oldEpoch);
        pauseReads = false;
        yield* Deferred.succeed(resume, undefined);
        yield* until(
          Effect.gen(function* () {
            return { epoch: yield* state.get("epoch"), ready: yield* state.get("ready") };
          }),
          (value) => value.epoch === snapshot.epoch && value.ready === "true",
        );
        expect(bootstraps).toBe(2);
        expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("replacement index");
        expect(yield* fs.readFileString(`${mirror}/.folder-sync/rejected/docs/a.md`)).toBe(
          "old index",
        );
        expect(yield* state.get("cursor")).toBe(oldCursor);
        expect(yield* state.pending()).toEqual([]);
        yield* Fiber.interrupt(daemon);
        yield* replacement.stop;
      }),
    { timeout: 20000 },
  );

  for (const { writable, older } of [
    { writable: true, older: false },
    { writable: false, older: false },
    { writable: true, older: true },
  ]) {
    it.effect(
      `a new feed epoch preserves synced files after an ${older ? "older" : "empty"} replacement in a ${writable ? "writable" : "read-only"} folder`,
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("sync-epoch-empty-");
          const mirror = `${directory}/mirror`;
          const files = older ? ["a.md", "b.md", "c.md"] : ["a.md", "b.md"];
          const fs = yield* FileSystem.FileSystem;
          yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

          const server = yield* spawnServer({
            cwd: `${import.meta.dir}/../..`,
            entry: "server/index.ts",
            env: { FOLDER_SYNC_DB: `${directory}/old.sqlite` },
          });

          yield* create(server.url, "docs");

          for (const file of files)
            yield* put(server.url, "docs", file, `original ${file}`, `old-${file}`);

          const context = yield* Layer.build(
            Layer.merge(
              stateLayer(`${mirror}/.folder-sync/state.sqlite`),
              remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
            ),
          );

          const state = yield* SyncState.pipe(Effect.provideContext(context));
          const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
          yield* until(remote.snapshot, (snapshot) => snapshot.files.length === files.length);
          const blocked = yield* Deferred.make<void>();
          const resume = yield* Deferred.make<void>();
          const deliver = yield* Deferred.make<void>();
          let pauseReads = false;
          let pauseWrites = false;

          const read = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            Effect.gen(function* () {
              if (pauseReads) {
                yield* Deferred.succeed(blocked, undefined);
                yield* Deferred.await(resume);
              }

              return yield* effect;
            });

          const daemon = yield* runSync({
            server: server.url,
            directory: mirror,
            folders: ["docs"],
            writable: writable ? ["docs"] : [],
          }).pipe(
            Effect.provideService(SyncRemote, {
              ...remote,
              snapshot: read(remote.snapshot),
              tail: read(remote.tail),
              head: (...args) => read(remote.head(...args)),
              page: (cursor) => read(remote.page(cursor, false)),
              push: (...args) =>
                Effect.gen(function* () {
                  if (pauseWrites) yield* Deferred.await(deliver);

                  return yield* remote.push(...args);
                }),
            }),
            Effect.provideContext(context),
            Effect.forkScoped,
          );

          yield* until(state.get("ready"), (value) => value === "true");
          const oldEpoch = yield* state.get("epoch");

          for (const file of files)
            expect(yield* fs.readFileString(`${mirror}/docs/${file}`)).toBe(`original ${file}`);
          pauseReads = true;
          pauseWrites = true;
          yield* Deferred.await(blocked);
          yield* server.stop;

          const replacement = yield* spawnServer({
            cwd: `${import.meta.dir}/../..`,
            entry: "server/index.ts",
            port: server.port,
            env: { FOLDER_SYNC_DB: `${directory}/new.sqlite` },
          });

          if (older) {
            yield* create(replacement.url, "docs");
            yield* put(replacement.url, "docs", "a.md", "original a.md", "same");
            yield* put(replacement.url, "docs", "b.md", "older b.md", "different");
          }

          const snapshot = yield* until(
            remote.snapshot,
            (value) => value.files.length === (older ? 2 : 0),
          );

          if (!older) expect(snapshot.files).toEqual([]);
          expect(snapshot.epoch).not.toBe(oldEpoch);
          pauseReads = false;
          yield* Deferred.succeed(resume, undefined);
          yield* until(
            Effect.gen(function* () {
              return { epoch: yield* state.get("epoch"), ready: yield* state.get("ready") };
            }),
            (value) => value.epoch === snapshot.epoch && value.ready === "true",
          );
          const pending = yield* state.pending();

          if (writable) {
            expect(pending).toHaveLength(2);

            const origin = yield* Schema.decodeUnknownEffect(Schema.String)(
              yield* state.get("origin"),
            );

            const conflict = conflictPath("b.md", origin);

            const uploads = older
              ? [
                  { path: "c.md", bytes: "original c.md" },
                  { path: conflict, bytes: "original b.md" },
                ]
              : files.map((file) => ({ path: file, bytes: `original ${file}` }));

            for (const upload of uploads) {
              expect(yield* fs.readFileString(`${mirror}/docs/${upload.path}`)).toBe(upload.bytes);
              expect(pending.find((entry) => entry.path === upload.path)).toMatchObject({
                base: null,
                bytes: text(upload.bytes),
              });
            }

            if (older) {
              expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("original a.md");
              expect(yield* fs.readFileString(`${mirror}/docs/b.md`)).toBe("older b.md");
            }

            // Empty replacements have no registry either. Only recreate the folder
            // after the local bytes and base-null outbox have been verified.
            if (!older) yield* create(replacement.url, "docs");
            pauseWrites = false;
            yield* Deferred.succeed(deliver, undefined);
            yield* until(state.pending(), (entries) => entries.length === 0);

            for (const upload of uploads) {
              expect((yield* remote.head("docs", upload.path)).bytes).toEqual(text(upload.bytes));
              const rows = yield* history(replacement.url, "docs", upload.path);
              expect(rows).toHaveLength(1);
              expect(rows[0]?.operation.base).toBeNull();
              expect(rows[0]?.operation.overwrote).toBeUndefined();
            }

            if (older) {
              // Matching heads are adopted and differing heads remain authoritative.
              expect(yield* history(replacement.url, "docs", "a.md")).toHaveLength(1);
              expect(yield* history(replacement.url, "docs", "b.md")).toHaveLength(1);
              expect((yield* remote.head("docs", "b.md")).bytes).toEqual(text("older b.md"));
            }
          } else {
            expect(pending).toEqual([]);

            for (const file of files) {
              expect(yield* fs.readFileString(`${mirror}/.folder-sync/rejected/docs/${file}`)).toBe(
                `original ${file}`,
              );
              expect(yield* fs.exists(`${mirror}/docs/${file}`)).toBe(false);
              expect((yield* remote.head("docs", file)).bytes).toBeNull();
            }
          }

          yield* Fiber.interrupt(daemon);
          yield* replacement.stop;
        }),
      { timeout: 20000 },
    );
  }

  it.effect(
    "writer authorization failures stop the daemon and leave its write in the outbox",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-auth-");
        const fs = yield* FileSystem.FileSystem;

        const server = yield* spawnServer({
          cwd: `${import.meta.dir}/../..`,
          entry: "server/index.ts",
        });

        yield* create(server.url, "docs");
        yield* put(server.url, "docs", "a.md", "initial", "initial");

        for (const status of [401, 403]) {
          const mirror = `${directory}/mirror-${status}`;
          yield* fs.makeDirectory(`${mirror}/.folder-sync`, { recursive: true });

          const context = yield* Layer.build(
            Layer.merge(
              stateLayer(`${mirror}/.folder-sync/state.sqlite`),
              remoteLayer(server.url, token).pipe(Layer.provide(FetchHttpClient.layer)),
            ),
          );

          const state = yield* SyncState.pipe(Effect.provideContext(context));
          const remote = yield* SyncRemote.pipe(Effect.provideContext(context));

          const daemon = yield* runSync({
            server: server.url,
            directory: mirror,
            folders: ["docs"],
            writable: ["docs"],
            settleMillis: 20,
          }).pipe(
            Effect.provideService(SyncRemote, {
              ...remote,
              push: () =>
                Effect.fail(
                  new RemoteFailure({
                    status,
                    retryable: false,
                    message: "Writer authorization failed; pending writes remain queued",
                  }),
                ),
            }),
            Effect.provideContext(context),
            Effect.forkScoped,
          );

          yield* until(state.get("ready"), (value) => value === "true");
          yield* until(
            fs.readFileString(`${mirror}/docs/a.md`).pipe(Effect.orElseSucceed(() => "<missing>")),
            (value) => value === "initial",
          );
          yield* fs.writeFileString(`${mirror}/docs/a.md`, "pending edit");
          const failure = yield* Effect.flip(Fiber.join(daemon));
          expect(failure).toMatchObject({ status });
          expect(yield* state.pending()).toHaveLength(1);
          expect(yield* state.rejected()).toEqual([]);
          expect(yield* fs.readFileString(`${mirror}/docs/a.md`)).toBe("pending edit");
        }

        yield* server.stop;
      }),
    { timeout: 20000 },
  );
});
