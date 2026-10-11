import { BunCrypto, BunServices } from "@effect/platform-bun";
import {
  Clock,
  Crypto,
  Duration,
  Effect,
  FileSystem,
  Layer,
  Path,
  Schedule,
  Schema,
  Predicate,
  Semaphore,
  Stream,
} from "effect";
import { FetchHttpClient } from "effect/http";
import { normalisePath, sha256 } from "../server/domain.ts";
import { SyncRemote, remoteLayer } from "./client.ts";
import {
  applyChange,
  conflictBase,
  feedReset,
  folderDirectory,
  isEcho,
  ignoredPath,
  conflictPath,
  backupPath,
} from "./decisions.ts";
import type { SyncOptions } from "./decisions.ts";
import {
  LocalFailure,
  SkippedLocal,
  localBytes,
  localFiles,
  replaceLocal,
  safePath,
  pathKind,
  skipWarnings,
} from "./local.ts";
import { SyncState, stateLayer } from "./state.ts";
import type { Known } from "./state.ts";

const skipLocal =
  (warn: (file: string, message: string) => Effect.Effect<void>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.catchIf(Schema.is(SkippedLocal), (error) => warn(error.path, error.message)),
    );

const hashBytes = (bytes: Uint8Array | null) =>
  bytes === null ? Effect.succeed(null) : sha256(bytes);

const retryRemote = <A, E, R>(work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.retry({
      while: (error) =>
        Schema.is(Schema.Struct({ retryable: Schema.Literal(true) }))(error) ||
        Predicate.isTagged("HttpClientError")(error),
      schedule: Schedule.exponential("100 millis").pipe(
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Math.min(Duration.toMillis(duration), 3000)),
        ),
      ),
    }),
  );

/** One owner serializes mirror decisions; network long-polls and outbox sends stay outside it. */
export const runSync = Effect.fn("FolderSync.run")(function* (options: SyncOptions) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const root = path.resolve(options.directory);
  const state = yield* SyncState;
  const remote = yield* SyncRemote;
  const gate = yield* Semaphore.make(1);
  const warn = yield* skipWarnings;
  const skip = skipLocal(warn);

  const selected = (folderId: string) =>
    options.folders === undefined || options.folders.includes(folderId);

  const writable = (folderId: string) => options.writable.includes(folderId);
  const mapping = options.folderDirectory ?? folderDirectory;
  const roots = new Map<string, string>();

  const configuration = yield* Schema.encodeEffect(
    Schema.fromJsonString(
      Schema.Struct({
        server: Schema.String,
        folders: Schema.NullOr(Schema.Array(Schema.String)),
        writable: Schema.Array(Schema.String),
      }),
    ),
  )({
    server: options.server,
    folders: options.folders === undefined ? null : [...options.folders].toSorted(),
    writable: [...options.writable].toSorted(),
  });

  const stored = yield* state.get("configuration");

  if (stored !== undefined && stored !== configuration)
    return yield* new LocalFailure({
      message: "This mirror belongs to another server or selection. Use another --dir.",
    });
  yield* state.set("configuration", configuration);
  let origin = yield* state.get("origin");

  if (origin === undefined) {
    origin = yield* crypto.randomUUIDv4;
    yield* state.set("origin", origin);
  }

  const clientId = origin;

  const addFolder = Effect.fn("Sync.addFolder")(function* (folderId: string) {
    if (!selected(folderId) || roots.has(folderId)) return yield* Effect.void;
    const relative = yield* normalisePath(mapping(folderId));

    if (relative === ".folder-sync" || relative.startsWith(".folder-sync/"))
      return yield* new LocalFailure({
        message: "Folder mapping uses reserved .folder-sync directory",
      });

    for (const [id, existing] of roots) {
      if (
        relative === existing ||
        relative.startsWith(`${existing}/`) ||
        existing.startsWith(`${relative}/`)
      )
        return yield* new LocalFailure({
          message: `Overlapping folder directories: ${id} and ${folderId}. Supply disjoint folderDirectory mappings.`,
        });
    }

    const previous = yield* state.get(`mapping:${folderId}`);

    if (previous !== undefined && previous !== relative)
      return yield* new LocalFailure({
        message: `Folder mapping changed for ${folderId}. Use another directory.`,
      });
    yield* state.set(`mapping:${folderId}`, relative);
    yield* state.addFolder(folderId);
    roots.set(folderId, relative);
    yield* safePath(root, relative);

    return yield* fs.makeDirectory(path.join(root, relative), { recursive: true });
  });

  for (const folderId of [
    ...(yield* state.folders()),
    ...(options.folders ?? []),
    ...options.writable,
  ])
    yield* addFolder(folderId);

  const filePath = (folderId: string, file: string) => path.join(mapping(folderId), file);

  const hasPending = Effect.fn("Sync.hasPending")(function* (
    folderId: string,
    file: string,
    exceptSeq?: number,
  ) {
    return (yield* state.pending()).some(
      (op) => op.folderId === folderId && op.path === file && op.seq !== exceptSeq,
    );
  });

  const install = Effect.fn("Sync.install")(function* (
    folderId: string,
    file: string,
    head: { version: string | null; hash: string | null; bytes: Uint8Array | null },
    exceptSeq?: number,
    force = false,
    unknownServer = false,
  ) {
    // An epoch change invalidates old echo hashes as well as remote versions.
    // Keep pending observations in storage, but adopt unqueued paths as fresh files.
    const known = unknownServer ? undefined : yield* state.head(folderId, file);

    if (!force && head.version !== null && known !== undefined && !applyChange(known, head.version))
      return yield* Effect.void;

    if (yield* hasPending(folderId, file, exceptSeq)) return yield* Effect.void;
    // Recheck after the network read: an editor may have changed bytes since observation.
    const local = yield* localBytes(root, filePath(folderId, file));
    const localHash = yield* hashBytes(local);

    if (
      !writable(folderId) &&
      local !== null &&
      localHash !== head.hash &&
      !isEcho(localHash, known)
    ) {
      const relative = path.join(".folder-sync", "rejected", mapping(folderId), file);
      let backup = relative;
      let suffix = 0;

      while (yield* fs.exists(path.join(root, backup))) {
        suffix++;
        backup = backupPath(relative, suffix);
      }

      const destination = yield* safePath(root, backup);
      yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
      yield* fs.rename(path.join(root, filePath(folderId, file)), destination);
      yield* Effect.logWarning(`Preserved read-only edit: ${destination}`);
    }

    const adopt = known === undefined && head.version !== null;

    if (adopt && writable(folderId) && local !== null && localHash !== head.hash) {
      let suffix = 0;
      let copy = conflictPath(file, clientId);

      while (yield* fs.exists(path.join(root, filePath(folderId, copy)))) {
        suffix++;
        copy = conflictPath(file, clientId, suffix);
      }

      yield* replaceLocal(root, filePath(folderId, copy), local);
      yield* Effect.logWarning(`Preserved bootstrap conflict: ${folderId}/${copy}`);
    }

    if (!adopt && writable(folderId) && !isEcho(localHash, known, exceptSeq !== undefined)) {
      const opId = yield* crypto.randomUUIDv4;
      yield* state.enqueue(
        { folderId, path: file, opId, base: conflictBase(known), hash: localHash, bytes: local },
        {
          folderId,
          path: file,
          version: known?.version ?? null,
          hash: known?.hash ?? null,
          localHash,
          localVersion: known?.localVersion ?? null,
        },
      );

      return yield* Effect.void;
    }

    const row: Known = {
      folderId,
      path: file,
      version: head.version,
      hash: head.hash,
      localHash: known?.localHash ?? null,
      localVersion: known?.localVersion ?? null,
    };

    // Write intent precedes the disk rename. Cursor settlement follows both disk and observation.
    yield* state.save(row);
    yield* replaceLocal(root, filePath(folderId, file), head.bytes);

    return yield* state.save({ ...row, localHash: head.hash, localVersion: head.version });
  }, skip);

  const observe = Effect.fn("Sync.observe")(function* (folderId: string, file: string) {
    if (ignoredPath(file)) return yield* Effect.void;
    const normalized = yield* normalisePath(file);
    const bytes = yield* localBytes(root, filePath(folderId, normalized));
    const hash = yield* hashBytes(bytes);
    const known = yield* state.head(folderId, normalized);

    const pending = yield* hasPending(folderId, normalized);

    if (known !== undefined && !pending && known.localVersion !== known.version) {
      // An interrupted pull recorded its intent before replacing disk bytes. Finish it even if
      // the replayed feed version equals the saved head, while preserving edits based on localVersion.
      const head = yield* remote.head(folderId, normalized, known.version ?? undefined);
      yield* install(folderId, normalized, head, undefined, true);

      return yield* Effect.void;
    }

    if (isEcho(hash, known, pending)) return yield* Effect.void;

    if (!writable(folderId)) {
      yield* Effect.logWarning(`Restoring read-only file: ${folderId}/${normalized}`);
      const head = yield* remote.head(folderId, normalized);
      yield* install(folderId, normalized, head, undefined, true);

      return yield* Effect.void;
    }

    const opId = yield* crypto.randomUUIDv4;
    yield* state.enqueue(
      { folderId, path: normalized, opId, base: conflictBase(known), hash, bytes },
      {
        folderId,
        path: normalized,
        version: known?.version ?? null,
        hash: known?.hash ?? null,
        localHash: hash,
        localVersion: known?.localVersion ?? null,
      },
    );

    return yield* Effect.void;
  }, skip);

  const scan = Effect.fn("Sync.scan")(function* () {
    for (const [folderId, relative] of roots) {
      const files = new Set(yield* localFiles(path.join(root, relative), warn));

      for (const head of yield* state.heads())
        if (head.folderId === folderId && !ignoredPath(head.path)) files.add(head.path);

      for (const file of files)
        yield* observe(folderId, file).pipe(
          Effect.catchTags({
            RemoteFailure: (error) => (error.retryable ? Effect.void : Effect.fail(error)),
            HttpClientError: () => Effect.void,
          }),
        );
    }

    yield* state.set("lastScan", String(yield* Clock.currentTimeMillis));
  });

  const bootstrap = Effect.fn("Sync.bootstrap")(function* () {
    yield* state.set("ready", "false");
    const snapshot = yield* remote.snapshot;
    yield* gate.withPermit(
      Effect.gen(function* () {
        const oldEpoch = yield* state.get("epoch");

        const epochChanged = oldEpoch !== undefined && oldEpoch !== snapshot.epoch;

        if (epochChanged) yield* state.resetVersions;

        for (const row of snapshot.files) yield* addFolder(row.folderId);
        const paths = new Map<string, { folderId: string; path: string }>();

        for (const row of [...snapshot.files, ...(yield* state.heads())]) {
          if (selected(row.folderId) && !ignoredPath(row.path))
            paths.set(`${row.folderId}\0${row.path}`, row);
        }

        for (const [folderId, relative] of roots) {
          for (const file of yield* localFiles(path.join(root, relative), warn))
            paths.set(`${folderId}\0${file}`, { folderId, path: file });
        }

        for (const row of paths.values()) {
          const head = yield* remote.head(row.folderId, row.path);

          if (yield* hasPending(row.folderId, row.path)) continue;
          yield* install(row.folderId, row.path, head, undefined, true, epochChanged);
        }

        yield* scan();
        yield* state.set("epoch", snapshot.epoch);
        yield* state.set("cursor", snapshot.cursor);
        yield* state.set("ready", "true");
      }),
    );
  });

  const pull = Effect.fn("Sync.pull")(function* () {
    let offset = yield* state.get("cursor");

    if (offset === undefined) {
      yield* bootstrap();
      offset = yield* state.get("cursor");
    }

    if (offset === undefined)
      return yield* new LocalFailure({ message: "Bootstrap did not save a cursor" });
    const tail = yield* remote.tail;

    if (feedReset(offset, tail.cursor) || tail.epoch !== (yield* state.get("epoch"))) {
      yield* Effect.logWarning("Change feed reset; bootstrapping again");
      yield* bootstrap();

      return yield* Effect.void;
    }

    let cursor: string = offset;
    let live = true;

    do {
      const page: Effect.Success<ReturnType<typeof remote.page>> = yield* remote.page(cursor, live);

      if (feedReset(cursor, page.cursor) || page.epoch !== (yield* state.get("epoch"))) {
        yield* bootstrap();

        return yield* Effect.void;
      }

      yield* gate.withPermit(
        Effect.gen(function* () {
          for (const change of page.rows) {
            if (!selected(change.folderId) || ignoredPath(change.path)) continue;
            yield* addFolder(change.folderId);
            const known = yield* state.head(change.folderId, change.path);

            if (!applyChange(known, change.version)) continue;

            if (known !== undefined) yield* observe(change.folderId, change.path);

            if (yield* hasPending(change.folderId, change.path)) continue;

            const head = change.deleted
              ? { version: change.version, hash: null, bytes: null }
              : yield* remote.head(change.folderId, change.path, change.version);

            yield* install(change.folderId, change.path, head);
          }

          yield* state.set("cursor", page.cursor);
        }),
      );
      cursor = page.cursor;
      live = page.upToDate;
    } while (!live);

    return yield* Effect.void;
  });

  const deliver = Effect.fn("Sync.deliver")(function* () {
    const operation = (yield* state.pending())[0];

    if (operation === undefined) return yield* Effect.void;

    const receipt = yield* remote.push(operation, clientId).pipe(
      Effect.catchTag("RemoteFailure", (error) =>
        Effect.gen(function* () {
          if (
            error.status === 401 ||
            error.status === 403 ||
            error.retryable ||
            error.status < 400 ||
            error.status >= 500
          )
            return yield* error;
          yield* gate.withPermit(state.reject(operation.seq, error.status, error.message));
          yield* Effect.logWarning(
            `Rejected write saved in SQLite: ${operation.folderId}/${operation.path} (${error.status})`,
          );

          return undefined;
        }),
      ),
    );

    if (receipt === undefined) return yield* Effect.void;

    if (receipt.overwrote !== undefined)
      yield* Effect.logWarning(
        `Replaced unseen version ${receipt.overwrote}: ${operation.folderId}/${operation.path}`,
      );

    yield* gate.withPermit(
      Effect.gen(function* () {
        // Cover changes skipped while this entry was pending before releasing its receipt.
        const head = yield* remote.head(
          operation.folderId,
          operation.path,
          receipt.version ?? undefined,
        );

        yield* observe(operation.folderId, operation.path);

        if (!(yield* hasPending(operation.folderId, operation.path, operation.seq)))
          yield* install(operation.folderId, operation.path, head, operation.seq, true);
        // Retain the replayable receipt until both disk replacement and known-head state have settled.
        yield* state.dropAndRebase(
          operation.seq,
          operation,
          operation.base,
          receipt.version ?? head.version,
        );

        return yield* Effect.void;
      }),
    );

    return yield* Effect.void;
  });

  let lastEvent = 0;
  const settle = options.settleMillis ?? 100;

  const scanSafely = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;

    if (now - lastEvent < settle || (yield* state.get("cursor")) === undefined) return;
    yield* gate.withPermit(scan());
  });

  const watch = fs.watch(root, { recursive: true }).pipe(
    Stream.filter(
      (event) => event.path.split(path.sep)[0] !== ".folder-sync" && !ignoredPath(event.path),
    ),
    Stream.tap(() =>
      Effect.gen(function* () {
        lastEvent = yield* Clock.currentTimeMillis;
      }),
    ),
    Stream.debounce(settle),
    Stream.runForEach(() => scanSafely),
  );

  // Reconciliation also covers watcher event coalescing and newly created subdirectories.
  yield* Effect.all(
    [
      watch,
      scanSafely.pipe(Effect.repeat(Schedule.spaced("1 second"))),
      retryRemote(pull()).pipe(Effect.repeat(Schedule.forever)),
      retryRemote(deliver()).pipe(Effect.repeat(Schedule.spaced("100 millis"))),
    ],
    { concurrency: "unbounded", discard: true },
  );

  return yield* Effect.void;
});

/** Resources close on interruption, including the watcher, long-poll and SQLite connection. */
export const sync = (options: SyncOptions) => {
  const state = Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = path.resolve(options.directory);
      yield* fs.makeDirectory(root, { recursive: true });
      yield* safePath(root, ".folder-sync");

      if ((yield* pathKind(path.join(root, ".folder-sync"))) === "file")
        return yield* new LocalFailure({ message: "Reserved .folder-sync path is a file" });
      yield* fs.makeDirectory(path.join(root, ".folder-sync"), { recursive: true });
      yield* safePath(root, ".folder-sync/state.sqlite");

      return stateLayer(path.join(root, ".folder-sync", "state.sqlite"));
    }),
  ).pipe(Layer.provide(BunServices.layer));

  const remote = remoteLayer(options.server, options.token).pipe(
    Layer.provide(FetchHttpClient.layer),
  );

  const runtime = Layer.mergeAll(state, remote, BunServices.layer, BunCrypto.layer);

  return runSync(options).pipe(
    Effect.scoped,
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This exported runner owns one scoped CLI runtime, also used by integration tests.
    Effect.provide(runtime),
  );
};
