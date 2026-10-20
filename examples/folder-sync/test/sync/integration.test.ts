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
import { cacheBody, captureLocal, replaceSource } from "../../sync/local.ts";
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

const registryPageCursor = Schema.decodeSync(Offset)("0000000000000001_0000000000000000");

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
  it.effect("writer authorization failures stop the daemon and leave its write in the outbox", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-unauthorized-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      const mirror = `${directory}/mirror`;
      const state = yield* initialiseMirror(mirror);

      const context = yield* Layer.build(
        remoteLayer(server.url, "wrong-token").pipe(Layer.provide(FetchHttpClient.layer)),
      );

      const remote = yield* SyncRemote.pipe(Effect.provideContext(context));

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, remote),
        Effect.forkScoped,
      );

      yield* until(state.get("ready"), (value) => value === "true");
      yield* fs.writeFileString(`${mirror}/daily/demo/private.md`, "keep me");
      const error = yield* Fiber.join(daemon).pipe(Effect.flip);
      expect(error).toMatchObject({ status: 401, retryable: false });
      expect(yield* read(server.url, "daily/demo", "private.md")).toBe("<missing>");
      const pending = yield* state.pending();
      expect(pending).toHaveLength(1);
      expect(pending[0]).toMatchObject({ path: "private.md", base: null });
      expect(yield* state.rejected()).toEqual([]);
      const source = pending[0]?.source;

      if (source === undefined || source === null)
        return yield* Effect.die("Missing retained bytes");
      expect(yield* fs.readFileString(source)).toBe("keep me");

      return yield* Effect.void;
    }),
  );

  it.effect("an offline create then delete preserves an unseen remote write", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-unseen-delete-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      const mirror = `${directory}/mirror`;
      const state = yield* initialiseMirror(mirror);
      const remote = yield* remoteFor(server.url);
      const release = yield* Deferred.make<void>();
      let deletes = 0;

      const offline = SyncRemote.of({
        ...remote,
        push: (op) =>
          Deferred.await(release).pipe(
            Effect.andThen(() => {
              if (op.source === null) deletes++;

              return remote.push(op);
            }),
          ),
      });

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, offline),
        Effect.forkScoped,
      );

      yield* until(state.get("ready"), (value) => value === "true");
      yield* fs.writeFileString(`${mirror}/daily/demo/n.md`, "mine");
      yield* until(
        state.pending(),
        (rows) => rows.filter((row) => row.path === "n.md").length === 1,
      );
      yield* fs.remove(`${mirror}/daily/demo/n.md`);
      yield* until(
        state.pending(),
        (rows) => rows.filter((row) => row.path === "n.md").length === 2,
      );
      yield* put(server.url, "daily/demo", "n.md", "theirs", "theirs");
      yield* Deferred.succeed(release, undefined);
      yield* until(state.pending(), (rows) => rows.length === 0);
      yield* until(
        fs
          .readFileString(`${mirror}/daily/demo/n.md`)
          .pipe(Effect.orElseSucceed(() => "<missing>")),
        (value) => value === "theirs",
      );
      expect(yield* read(server.url, "daily/demo", "n.md")).toBe("theirs");
      const origin = yield* state.get("origin");

      if (origin === undefined) return yield* Effect.die("Missing origin");
      expect(yield* read(server.url, "daily/demo", conflictPath("n.md", origin))).toBe("mine");
      expect(deletes).toBe(0);
      yield* Fiber.interrupt(daemon);

      return yield* Effect.void;
    }),
  );

  it.effect("a delete rejected with 412 preserves the base of later queued deletes", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-stale-delete-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      const original = yield* put(server.url, "daily/demo", "n.md", "original", "original");
      const mirror = `${directory}/mirror`;
      const state = yield* initialiseMirror(mirror);
      const remote = yield* remoteFor(server.url);
      const release = yield* Deferred.make<void>();
      const bases: Array<number | null> = [];

      const offline = SyncRemote.of({
        ...remote,
        push: (op) =>
          Deferred.await(release).pipe(
            Effect.andThen(() => {
              if (op.source === null) bases.push(op.base);

              return remote.push(op);
            }),
          ),
      });

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, offline),
        Effect.forkScoped,
      );

      yield* until(state.get("ready"), (value) => value === "true");
      yield* fs.remove(`${mirror}/daily/demo/n.md`);
      const queued = yield* until(state.pending(), (rows) => rows.length === 1);
      const deletion = queued[0];
      const known = yield* state.head("daily/demo", "n.md");

      if (deletion === undefined || known === undefined)
        return yield* Effect.die("Missing deletion");
      // A second deletion observed before the first request returned keeps the same base.
      yield* state.enqueue(
        {
          folderId: deletion.folderId,
          path: deletion.path,
          txid: "later-delete",
          base: deletion.base,
          hash: deletion.hash,
          size: deletion.size,
          source: deletion.source,
          uploaded: deletion.uploaded,
        },
        known,
      );
      yield* put(server.url, "daily/demo", "n.md", "theirs", "theirs");
      yield* Deferred.succeed(release, undefined);
      yield* until(state.pending(), (rows) => rows.length === 0);
      yield* until(
        fs
          .readFileString(`${mirror}/daily/demo/n.md`)
          .pipe(Effect.orElseSucceed(() => "<missing>")),
        (value) => value === "theirs",
      );
      expect(bases).toEqual([original.version, original.version]);
      expect(yield* read(server.url, "daily/demo", "n.md")).toBe("theirs");
      yield* Fiber.interrupt(daemon);

      return yield* Effect.void;
    }),
  );

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

  for (const editedWhileStopped of [false, true])
    it.effect(
      `conflict retarget crash with a current cursor recovers ${editedWhileStopped ? "and preserves stopped edits" : "the original path"}`,
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("sync-retarget-restart-");
          const server = yield* host(directory);
          const fs = yield* FileSystem.FileSystem;
          yield* create(server.url, "daily/demo");
          yield* create(server.url, "ro");
          const initial = yield* put(server.url, "daily/demo", "a.md", "baseline", "initial");
          const mirror = `${directory}/mirror`;
          const state = yield* initialiseMirror(mirror);
          yield* fs.makeDirectory(`${mirror}/daily/demo`, { recursive: true });
          yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "conflicted local bytes");
          const content = yield* captureLocal(mirror, "daily/demo/a.md");

          if (content === null) return yield* Effect.die("Missing local fixture");
          const human = yield* put(server.url, "daily/demo", "a.md", "human head", "human");
          const humanContent = yield* prepareContent(text("human head"));
          yield* state.enqueue(
            {
              folderId: "daily/demo",
              path: "a.md",
              txid: "attempt",
              base: initial.version,
              ...content,
              uploaded: 0,
            },
            {
              folderId: "daily/demo",
              path: "a.md",
              version: human.version,
              hash: humanContent.hash,
              localHash: content.hash,
              localVersion: initial.version,
            },
          );
          yield* state.addFolder("daily/demo");
          yield* state.set("origin", "recovery-client");
          const remote = yield* remoteFor(server.url);
          const currentCursor = yield* remote.tail("daily/demo");
          yield* state.set("cursor:daily/demo", currentCursor);
          const op = (yield* state.pending())[0];

          if (op === undefined) return yield* Effect.die("Missing pending fixture");
          const copy = conflictPath("a.md", "recovery-client");
          // Exact durable boundary: retarget and disk copy landed, then the process stopped before refresh(original).
          yield* state.conflict(op.seq, copy, "copy-attempt");
          yield* replaceSource(mirror, `daily/demo/${copy}`, content.source);
          yield* state.save({
            folderId: "daily/demo",
            path: copy,
            version: null,
            hash: null,
            localHash: content.hash,
            localVersion: null,
          });
          yield* state.set(`restore:${op.seq}`, "");

          if (editedWhileStopped)
            yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "edited while stopped");

          let initialRead: string | undefined;

          const observed = SyncRemote.of({
            ...remote,
            page: (folder, offset, live) => {
              if (folder === "daily/demo" && initialRead === undefined) initialRead = offset;

              return remote.page(folder, offset, live);
            },
          });

          const daemon = yield* runSync(options(server.url, mirror)).pipe(
            Effect.provideService(SyncState, state),
            Effect.provideService(SyncRemote, observed),
            Effect.forkScoped,
          );

          yield* until(state.get("ready"), (ready) => ready === "true");
          expect(initialRead).toBe(currentCursor);

          if (!editedWhileStopped)
            expect(yield* fs.readFileString(`${mirror}/daily/demo/a.md`)).toBe("human head");
          yield* until(
            read(server.url, "daily/demo", copy),
            (value) => value === "conflicted local bytes",
          );

          if (editedWhileStopped)
            yield* until(
              read(server.url, "daily/demo", conflictPath("a.md", "recovery-client", 1)),
              (value) => value === "edited while stopped",
            );
          yield* until(
            fs.readFileString(`${mirror}/daily/demo/a.md`),
            (value) => value === "human head",
          );
          yield* until(state.pending(), (rows) => rows.length === 0);
          yield* Fiber.interrupt(daemon);

          return yield* Effect.void;
        }),
    );

  it.effect("an edit during a paused blob download becomes a committed conflict copy", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("sync-download-edit-");
      const server = yield* host(directory);
      const fs = yield* FileSystem.FileSystem;
      yield* create(server.url, "daily/demo");
      yield* create(server.url, "ro");
      yield* put(server.url, "daily/demo", "a.md", "old bytes", "initial");
      const mirror = `${directory}/mirror`;
      const state = yield* initialiseMirror(mirror);
      const remote = yield* remoteFor(server.url);
      const downloading = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const modified = SyncRemote.of({
        ...remote,
        body: (hash) =>
          remote
            .body(hash)
            .pipe(
              Effect.map((body) =>
                Stream.unwrap(
                  Deferred.succeed(downloading, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as(body),
                  ),
                ),
              ),
            ),
      });

      const daemon = yield* runSync(options(server.url, mirror)).pipe(
        Effect.provideService(SyncState, state),
        Effect.provideService(SyncRemote, modified),
        Effect.forkScoped,
      );

      yield* until(state.get("ready"), (ready) => ready === "true");
      const binary = new Uint8Array(128 * 1024).fill(3);
      const content = yield* prepareContent(binary);

      const uploaded = yield* HttpClient.put(`${server.url}/api/blobs/${content.hash}`, {
        body: HttpBody.uint8Array(binary),
      });

      expect(uploaded.status).toBe(200);
      yield* uploaded.text;

      const committed = yield* HttpClient.put(fileUrl(server.url, "daily/demo", "a.md"), {
        body: HttpBody.jsonUnsafe({ ...content, author: "test", txid: "remote-binary" }),
      });

      expect(committed.status).toBe(200);
      yield* committed.text;
      yield* Deferred.await(downloading);
      yield* fs.writeFileString(`${mirror}/daily/demo/a.md`, "edited during download");
      yield* Deferred.succeed(release, undefined);
      const origin = yield* state.get("origin");

      if (origin === undefined) return yield* Effect.die("Missing client identity");
      yield* until(
        read(server.url, "daily/demo", conflictPath("a.md", origin)),
        (value) => value === "edited during download",
      );
      yield* until(state.pending(), (rows) => rows.length === 0);
      expect(new Uint8Array(yield* fs.readFile(`${mirror}/daily/demo/a.md`))).toEqual(binary);
      yield* Fiber.interrupt(daemon);

      return yield* Effect.void;
    }),
  );

  it.effect(
    "registry paging holds readiness false until every existing folder and local create is caught up",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("sync-registry-pages-");
        const server = yield* host(directory);
        const fs = yield* FileSystem.FileSystem;
        yield* create(server.url, "daily/demo");
        yield* create(server.url, "ro");
        yield* put(server.url, "ro", "existing.md", "second page contents", "existing");
        const mirror = `${directory}/mirror`;
        const state = yield* initialiseMirror(mirror);
        yield* state.set("ready", "true");
        yield* fs.makeDirectory(`${mirror}/daily/demo`, { recursive: true });
        yield* fs.writeFileString(`${mirror}/daily/demo/new.md`, "present before bootstrap");
        const remote = yield* remoteFor(server.url);
        const page = yield* remote.folders("-1", false);
        const finalRegistryCursor = yield* Schema.decodeEffect(Offset)(page.cursor);
        const rows = page.rows.filter((row) => "type" in row);
        expect(rows).toHaveLength(2);
        const waiting = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let calls = 0;

        const modified = SyncRemote.of({
          ...remote,
          folders: (offset, live) =>
            Effect.gen(function* () {
              calls++;

              if (calls === 1)
                return {
                  ...page,
                  rows: rows.slice(0, 1),
                  upToDate: false,
                  cursor: registryPageCursor,
                };

              if (calls === 2) {
                expect(offset).toBe(registryPageCursor);
                expect(live).toBe(false);
                yield* Deferred.succeed(waiting, undefined);
                yield* Deferred.await(release);

                return {
                  ...page,
                  cursor: finalRegistryCursor,
                  rows: rows.slice(1),
                  upToDate: true,
                };
              }

              return yield* remote.folders(offset, live);
            }),
        });

        const daemon = yield* runSync({
          server: server.url,
          directory: mirror,
          writable: ["daily/demo"],
          token,
          settleMillis: 30,
        }).pipe(
          Effect.provideService(SyncState, state),
          Effect.provideService(SyncRemote, modified),
          Effect.forkScoped,
        );

        yield* Deferred.await(waiting);
        expect(yield* state.get("ready")).toBe("false");
        expect(yield* fs.exists(`${mirror}/ro/existing.md`)).toBe(false);
        yield* Deferred.succeed(release, undefined);
        yield* until(state.get("ready"), (ready) => ready === "true");
        expect(yield* fs.readFileString(`${mirror}/ro/existing.md`)).toBe("second page contents");
        yield* until(
          read(server.url, "daily/demo", "new.md"),
          (value) => value === "present before bootstrap",
        );
        expect(yield* fs.readFileString(`${mirror}/daily/demo/new.md`)).toBe(
          "present before bootstrap",
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
