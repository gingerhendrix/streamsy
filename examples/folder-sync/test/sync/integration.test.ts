import { Offset } from "@streamsy/core";
import { BunCrypto, BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Fiber, FileSystem, Layer, Schedule, Schema, Stream } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { prepareContent } from "../../server/domain.ts";
import { conflictPath, encodeFilePath } from "../../sync/decisions.ts";
import { runSync, sync } from "../../sync/engine.ts";
import { RemoteFailure, SyncRemote, remoteLayer } from "../../sync/client.ts";
import { cacheBody, captureLocal } from "../../sync/local.ts";
import { SyncState, stateLayer } from "../../sync/state.ts";
import { spawnServer, token } from "./host.ts";

class Waiting extends Schema.TaggedError<Waiting>()("SyncTestWaiting", {}) {}

const until = <A, E, R>(work: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  work.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 500 })),
    }),
  );

const text = (value: string) => new TextEncoder().encode(value);

const fileUrl = (server: string, folder: string, file: string) =>
  `${server}/api/folders/${encodeURIComponent(folder)}/files/${encodeFilePath(file)}`;

const create = Effect.fn("SyncTest.create")(function* (server: string, folderId: string) {
  const response = yield* HttpClient.post(`${server}/api/folders`, {
    body: HttpBody.jsonUnsafe({ folderId, title: folderId, txid: `create-${folderId}` }),
  });

  expect(response.status).toBe(200);
  yield* response.text;
});

const put = Effect.fn("SyncTest.put")(function* (
  server: string,
  folder: string,
  file: string,
  value: string,
  txid: string,
) {
  const content = yield* prepareContent(text(value));

  const response = yield* HttpClient.put(fileUrl(server, folder, file), {
    body: HttpBody.jsonUnsafe({ ...content, txid, author: "test" }),
  });

  expect(response.status).toBe(200);

  return yield* Schema.decodeUnknownEffect(
    Schema.Struct({ version: Schema.Int, txid: Schema.String }),
  )(yield* response.json);
});

const read = (server: string, folder: string, file: string) =>
  HttpClient.get(fileUrl(server, folder, file)).pipe(
    Effect.flatMap((response) =>
      response.status === 404 ? Effect.succeed("<missing>") : response.text,
    ),
  );

const inspect = Effect.fn("SyncTest.inspect")(function* (directory: string) {
  const context = yield* Layer.build(stateLayer(`${directory}/.folder-sync/state.sqlite`));

  return yield* SyncState.pipe(Effect.provideContext(context));
});

const clientLayer = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(HttpClient.HttpClient, (client) =>
    HttpClient.mapRequest(client, HttpClientRequest.bearerToken(token)),
  ),
).pipe(Layer.provide(FetchHttpClient.layer));

const runtime = Layer.mergeAll(BunServices.layer, BunCrypto.layer, clientLayer);

const host = (directory: string) =>
  spawnServer({
    cwd: `${import.meta.dirname}/../..`,
    entry: "server/index.ts",
    env: { FOLDER_SYNC_DB: `${directory}/server.sqlite` },
  });

const options = (
  server: string,
  directory: string,
  writable: ReadonlyArray<string> = ["daily/demo"],
) => ({ server, directory, folders: ["daily/demo", "ro"], writable, token, settleMillis: 30 });

const remoteFor = Effect.fn("SyncTest.remote")(function* (server: string) {
  const context = yield* Layer.build(
    remoteLayer(server, token).pipe(Layer.provide(FetchHttpClient.layer)),
  );

  return yield* SyncRemote.pipe(Effect.provideContext(context));
});

const initialiseMirror = Effect.fn("SyncTest.initialiseMirror")(function* (directory: string) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(`${directory}/.folder-sync`, { recursive: true });

  return yield* inspect(directory);
});

layer(runtime, { excludeTestServices: true })((it) => {
  it.effect(
    "two clients converge through create, edit, delete, restart and read-only edit preservation",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-protocol-");
        const server = yield* host(directory);
        const fs = yield* FileSystem.FileSystem;
        yield* create(server.url, "daily/demo");
        yield* create(server.url, "ro");
        yield* create(server.url, "excluded");
        yield* put(server.url, "daily/demo", "seed.md", "seed", "seed");
        yield* put(server.url, "ro", "read.md", "read only", "ro");
        const a = `${directory}/a`;
        const b = `${directory}/b`;
        let clientA = yield* sync(options(server.url, a)).pipe(Effect.forkScoped);
        yield* sync(options(server.url, b)).pipe(Effect.forkScoped);

        const local = (file: string) =>
          fs.readFileString(file).pipe(Effect.orElseSucceed(() => "<missing>"));

        yield* until(local(`${a}/daily/demo/seed.md`), (value) => value === "seed");
        yield* until(local(`${b}/daily/demo/seed.md`), (value) => value === "seed");
        expect(yield* fs.exists(`${a}/excluded`)).toBe(false);
        yield* fs.writeFileString(`${a}/daily/demo/new.md`, "created locally");
        yield* until(local(`${b}/daily/demo/new.md`), (value) => value === "created locally");
        yield* fs.writeFileString(`${b}/daily/demo/new.md`, "edited on second");
        yield* until(local(`${a}/daily/demo/new.md`), (value) => value === "edited on second");
        yield* fs.remove(`${a}/daily/demo/new.md`);
        yield* until(fs.exists(`${b}/daily/demo/new.md`), (exists) => !exists);
        yield* put(server.url, "daily/demo", "remote.md", "created remotely", "remote");
        yield* until(local(`${a}/daily/demo/remote.md`), (value) => value === "created remotely");
        yield* put(server.url, "daily/demo", "remote.md", "edited remotely", "remote-edit");
        yield* until(local(`${b}/daily/demo/remote.md`), (value) => value === "edited remotely");

        const deleted = yield* HttpClient.del(fileUrl(server.url, "daily/demo", "remote.md"), {
          body: HttpBody.jsonUnsafe({ txid: "remote-delete", author: "test" }),
        });

        expect(deleted.status).toBe(200);
        yield* until(fs.exists(`${a}/daily/demo/remote.md`), (exists) => !exists);
        yield* Fiber.interrupt(clientA);
        yield* put(server.url, "daily/demo", "seed.md", "while stopped", "offline");
        clientA = yield* sync(options(server.url, a)).pipe(Effect.forkScoped);
        yield* until(local(`${a}/daily/demo/seed.md`), (value) => value === "while stopped");
        yield* fs.writeFileString(`${a}/ro/read.md`, "preserve this edit");
        yield* until(local(`${a}/ro/read.md`), (value) => value === "read only");
        expect(yield* local(`${a}/.folder-sync/rejected/ro/read.md`)).toBe("preserve this edit");
        const state = yield* inspect(a);
        expect(yield* state.get("cursor:daily/demo")).toBeDefined();
        expect(yield* state.get("cursor:ro")).toBeDefined();
        expect(yield* state.get("epoch")).toBeUndefined();
        yield* Fiber.interrupt(clientA);
      }),
  );

  it.effect("a 412 preserves immutable local bytes in a committed conflict sibling", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-conflict-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      yield* put(server.url, "daily/demo", "a.md", "original", "original");
      const mirror = `${directory}/mirror`;
      const state = yield* initialiseMirror(mirror);
      const remote = yield* remoteFor(server.url);
      let raced = false;

      const modified = SyncRemote.of({
        ...remote,
        push: (op) =>
          Effect.gen(function* () {
            if (!raced && op.path === "a.md") {
              raced = true;
              yield* put(server.url, "daily/demo", "a.md", "human wins", "human").pipe(
                // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test fixture injects a separately owned authenticated request runtime.
                Effect.provide(runtime),
                Effect.orDie,
              );
            }

            return yield* remote.push(op);
          }),
      });

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, modified),
        Effect.forkScoped,
      );

      yield* until(state.get("ready"), (value) => value === "true");
      yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "local conflict");
      const origin = yield* state.get("origin");

      if (origin === undefined) return yield* Effect.die("Missing origin");
      const copy = conflictPath("a.md", origin);
      yield* until(read(server.url, "daily/demo", copy), (value) => value === "local conflict");
      yield* until(
        fs.readFileString(`${mirror}/daily/demo/a.md`),
        (value) => value === "human wins",
      );
      expect(yield* fs.readFileString(`${mirror}/daily/demo/${copy}`)).toBe("local conflict");
      expect(yield* state.rejected()).toEqual([]);
      yield* Fiber.interrupt(daemon);

      return yield* Effect.void;
    }),
  );

  it.effect(
    "pending content survives a working-file edit and a CLI restart; rejection retains its cache source",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-pending-");
        const server = yield* host(directory);
        const fs = yield* FileSystem.FileSystem;
        yield* create(server.url, "daily/demo");
        yield* create(server.url, "ro");
        const mirror = `${directory}/mirror`;
        const state = yield* initialiseMirror(mirror);
        const remote = yield* remoteFor(server.url);
        const waiting = yield* Deferred.make<void>();

        const offline = SyncRemote.of({
          ...remote,
          push: () => Deferred.succeed(waiting, undefined).pipe(Effect.andThen(Effect.never)),
        });

        const first = yield* runSync(options(server.url, mirror)).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, offline),
          Effect.forkScoped,
        );

        yield* until(state.get("ready"), (value) => value === "true");
        yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "captured first");
        yield* Deferred.await(waiting);
        const pending = (yield* state.pending())[0];
        expect(pending?.source).toBeDefined();

        if (pending?.source == null) return yield* Effect.die("Missing durable source");
        yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "changed later");
        expect(yield* fs.readFileString(pending.source)).toBe("captured first");
        yield* Fiber.interrupt(first);

        const resumed = yield* runSync(options(server.url, mirror)).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, remote),
          Effect.forkScoped,
        );

        yield* until(read(server.url, "daily/demo", "a.md"), (value) => value === "changed later");
        yield* until(state.pending(), (rows) => rows.length === 0);
        yield* Fiber.interrupt(resumed);

        const rejecting = SyncRemote.of({
          ...remote,
          push: () =>
            Effect.fail(
              new RemoteFailure({ status: 400, retryable: false, message: "rejected fixture" }),
            ),
        });

        const last = yield* runSync(options(server.url, mirror)).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, rejecting),
          Effect.forkScoped,
        );

        yield* fs.writeFileString(`${mirror}/daily/demo/rejected.md`, "retained rejected bytes");
        const rows = yield* until(state.rejected(), (rejected) => rejected.length === 1);
        expect(rows[0]?.size).toBe(23);
        const source = rows[0]?.source;

        if (source == null) return yield* Effect.die("Missing rejected source");
        expect(yield* fs.readFileString(source)).toBe("retained rejected bytes");
        yield* Fiber.interrupt(last);

        return yield* Effect.void;
      }),
  );

  it.effect(
    "an interrupted pull replays its intent without treating unchanged old disk bytes as an edit",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-interrupted-");
        const server = yield* host(directory);
        const fs = yield* FileSystem.FileSystem;
        yield* create(server.url, "daily/demo");
        yield* create(server.url, "ro");
        const initial = yield* put(server.url, "daily/demo", "a.md", "old bytes", "first");
        const mirror = `${directory}/mirror`;
        const first = yield* sync(options(server.url, mirror)).pipe(Effect.forkScoped);

        const state = yield* until(
          inspect(mirror).pipe(Effect.orElseSucceed(() => null)),
          (value) => value !== null,
        );

        if (state === null) return yield* Effect.die("Missing state");
        yield* until(state.get("ready"), (value) => value === "true");
        yield* Fiber.interrupt(first);
        const known = yield* state.head("daily/demo", "a.md");

        if (known === undefined) return yield* Effect.die("Missing known head");
        const next = yield* put(server.url, "daily/demo", "a.md", "new bytes", "second");
        const content = yield* prepareContent(text("new bytes"));
        yield* state.save({
          ...known,
          version: next.version,
          hash: content.hash,
          localVersion: initial.version,
        });
        expect(yield* fs.readFileString(`${mirror}/daily/demo/a.md`)).toBe("old bytes");
        const restarted = yield* sync(options(server.url, mirror)).pipe(Effect.forkScoped);
        yield* until(
          fs.readFileString(`${mirror}/daily/demo/a.md`),
          (value) => value === "new bytes",
        );
        expect(yield* state.pending()).toEqual([]);
        expect(
          (yield* fs.readDirectory(`${mirror}/daily/demo`)).filter((name) =>
            name.includes("conflict"),
          ),
        ).toEqual([]);
        yield* Fiber.interrupt(restarted);

        return yield* Effect.void;
      }),
  );

  for (const reset of ["410", "reset"] as const)
    it.effect(`${reset} resets a folder cursor and reconciles a full replay`, () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory(`sync-${reset}-`);
        const server = yield* host(directory);
        const fs = yield* FileSystem.FileSystem;
        yield* create(server.url, "daily/demo");
        yield* create(server.url, "ro");
        yield* put(server.url, "daily/demo", "a.md", "before", "before");
        const mirror = `${directory}/mirror`;
        const state = yield* initialiseMirror(mirror);
        const remote = yield* remoteFor(server.url);
        const injected = yield* Deferred.make<void>();
        let replayed = false;
        let resetSent = false;

        const modified = SyncRemote.of({
          ...remote,
          page: (folder, offset, live) =>
            Effect.gen(function* () {
              if (folder === "daily/demo" && live && !resetSent) {
                resetSent = true;
                yield* put(server.url, "daily/demo", "a.md", "after reset", "after").pipe(
                  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test fixture owns a separately authenticated request runtime.
                  Effect.provide(runtime),
                  Effect.orDie,
                );
                yield* Deferred.succeed(injected, undefined);

                return {
                  rows: reset === "reset" ? [{ headers: { control: "reset" as const } }] : [],
                  cursor: yield* remote.tail(folder),
                  reset: reset === "410",
                  upToDate: false,
                };
              }

              if (resetSent && offset === "-1") replayed = true;

              return yield* remote.page(folder, offset, live);
            }),
        });

        const daemon = yield* runSync(options(server.url, mirror)).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, modified),
          Effect.forkScoped,
        );

        yield* Deferred.await(injected);
        yield* until(
          fs.readFileString(`${mirror}/daily/demo/a.md`),
          (value) => value === "after reset",
        );
        expect(replayed).toBe(true);
        yield* Fiber.interrupt(daemon);
      }),
    );

  it.effect("HEAD rewind stops before changing disk; opaque folder mapping remains stable", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-rewind-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      yield* put(server.url, "daily/demo", "a.md", "kept", "first");
      const mirror = `${directory}/mirror`;

      const first = yield* sync({
        ...options(server.url, mirror),
        folderDirectory: (id) => (id === "daily/demo" ? "mapped" : id),
      }).pipe(Effect.forkScoped);

      const state = yield* until(
        inspect(mirror).pipe(Effect.orElseSucceed(() => null)),
        (value) => value !== null,
      );

      if (state === null) return yield* Effect.die("Missing state");
      yield* until(state.get("ready"), (value) => value === "true");
      yield* Fiber.interrupt(first);
      const remote = yield* remoteFor(server.url);

      const result = yield* runSync({
        ...options(server.url, mirror),
        folderDirectory: (id) => (id === "daily/demo" ? "mapped" : id),
      }).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, {
          ...remote,
          tail: () =>
            Effect.succeed(Schema.decodeSync(Offset)("0000000000000000_0000000000000000")),
        }),
        Effect.result,
      );

      expect(result).toMatchObject({ failure: { _tag: "HistoryRewound" } });
      expect(yield* fs.readFileString(`${mirror}/mapped/a.md`)).toBe("kept");

      const changed = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, remote),
        Effect.result,
      );

      expect(changed).toMatchObject({ failure: { _tag: "LocalFailure" } });

      return yield* Effect.void;
    }),
  );

  it.effect("binary upload and second-client pull recover an actual blob-missing 409", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-binary-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      const mirror = `${directory}/writer`;
      const reader = `${directory}/reader`;
      const state = yield* initialiseMirror(mirror);
      const remote = yield* remoteFor(server.url);
      let uploads = 0;

      const modified = SyncRemote.of({
        ...remote,
        upload: (op) => {
          uploads++;

          // Simulate a lost upload acknowledgment with no stored object. The commit then returns 409.
          return uploads === 1 ? Effect.void : remote.upload(op);
        },
      });

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, modified),
        Effect.forkScoped,
      );

      yield* sync(options(server.url, reader, [])).pipe(Effect.forkScoped);
      yield* until(state.get("ready"), (value) => value === "true");
      const bytes = new Uint8Array(512 * 1024);

      for (let i = 0; i < bytes.length; i++) bytes[i] = i % 251;
      yield* fs.writeFile(`${mirror}/daily/demo/large.bin`, bytes);
      const local = `${reader}/daily/demo/large.bin`;
      yield* until(fs.exists(local), (exists) => exists);
      expect(new Uint8Array(yield* fs.readFile(local))).toEqual(bytes);
      expect(uploads).toBe(2);
      yield* fs.writeFile(`${mirror}/daily/demo/nul.bin`, new Uint8Array([65, 0, 66]));
      yield* until(fs.exists(`${reader}/daily/demo/nul.bin`), (exists) => exists);
      expect(new Uint8Array(yield* fs.readFile(`${reader}/daily/demo/nul.bin`))).toEqual(
        new Uint8Array([65, 0, 66]),
      );
      yield* Fiber.interrupt(daemon);
    }),
  );

  it.effect(
    "symlinks and oversized files are skipped and large content cache snapshots remain immutable",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-safety-");
        const fs = yield* FileSystem.FileSystem;
        yield* fs.makeDirectory(`${directory}/.folder-sync`, { recursive: true });
        yield* fs.writeFileString(`${directory}/outside`, "never touched");
        yield* fs.symlink(`${directory}/outside`, `${directory}/link`);
        expect(yield* captureLocal(directory, "link").pipe(Effect.result)).toMatchObject({
          failure: { _tag: "SkippedLocal" },
        });
        const chunk = new Uint8Array(64 * 1024).fill(123);

        const cached = yield* cacheBody(
          directory,
          Stream.fromIterable(Array.from({ length: 32 }, () => chunk)),
        );

        expect(cached.size).toBe(2 * 1024 * 1024);
        yield* fs.writeFileString(`${directory}/working`, "newer working bytes");
        expect(Number((yield* fs.stat(cached.source)).size)).toBe(cached.size);

        const tooLarge = yield* cacheBody(
          directory,
          Stream.fromIterable(Array.from({ length: 1025 }, () => chunk)),
        ).pipe(Effect.result);

        expect(tooLarge).toMatchObject({ failure: { _tag: "SkippedLocal" } });
        expect(yield* fs.readFileString(`${directory}/outside`)).toBe("never touched");
      }),
  );
});
