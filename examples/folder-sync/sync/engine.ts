import { BunCrypto, BunServices } from "@effect/platform-bun";
import {
  Clock,
  Crypto,
  Duration,
  Effect,
  FileSystem,
  FiberSet,
  Layer,
  Path,
  Predicate,
  Schedule,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { FetchHttpClient } from "effect/http";
import { INLINE_LIMIT, inlineText, normalisePath, type File } from "../server/domain.ts";
import {
  afterLastReset,
  applyChanges,
  isReset,
  isUpsert,
  type FileMessage,
} from "../server/messages.ts";
import { RemoteFailure, SyncRemote, remoteLayer } from "./client.ts";
import {
  backupPath,
  conflictBase,
  conflictPath,
  folderDirectory,
  historyRewound,
  ignoredPath,
  isEcho,
  type SyncOptions,
} from "./decisions.ts";
import {
  cacheBody,
  captureLocal,
  LocalFailure,
  localFiles,
  pathKind,
  replaceSource,
  safePath,
  skipWarnings,
  SkippedLocal,
} from "./local.ts";
import { folderRoot } from "./layout.ts";
import { SyncState, stateLayer, type Known } from "./state.ts";

export class HistoryRewound extends Schema.TaggedError<HistoryRewound>()("HistoryRewound", {
  message: Schema.String,
}) {}

const retryRemote = <A, E, R>(work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.retry({
      while: (error) =>
        Schema.is(Schema.Struct({ retryable: Schema.Literal(true) }))(error) ||
        Predicate.isTagged(error, "HttpClientError"),
      schedule: Schedule.exponential("100 millis").pipe(
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Math.min(Duration.toMillis(duration), 3000)),
        ),
      ),
    }),
  );

/** One owner protects filesystem observations and durable cursor settlement. Network tails wait outside it. */
export const runSync = Effect.fn("FolderSync.run")(function* (options: SyncOptions) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const state = yield* SyncState;
  const remote = yield* SyncRemote;
  const root = path.resolve(options.directory);
  const gate = yield* Semaphore.make(1);
  const warn = yield* skipWarnings;

  const skip = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    work.pipe(Effect.catchIf(Schema.is(SkippedLocal), (error) => warn(error.path, error.message)));

  const selected = (id: string) => options.folders === undefined || options.folders.includes(id);
  const writable = (id: string) => options.writable.includes(id);
  const mapping = options.folderDirectory ?? folderDirectory;
  const roots = new Map<string, string>();

  const filePath = (folderId: string, file: string) =>
    path.join(roots.get(folderId) ?? mapping(folderId), file);

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
  yield* state.set("ready", "false");
  let origin = yield* state.get("origin");

  if (origin === undefined) {
    origin = yield* crypto.randomUUIDv4();
    yield* state.set("origin", origin);
  }

  const clientId = origin;

  const hasPending = Effect.fn("Sync.hasPending")(function* (folderId: string, file: string) {
    return (yield* state.pending()).some((op) => op.folderId === folderId && op.path === file);
  });

  const cache = Effect.fn("Sync.cache")(function* (value: File) {
    const source = yield* safePath(root, `.folder-sync/content/${value.hash}`);

    if (yield* fs.exists(source)) return { source, hash: value.hash, size: value.size };

    const content =
      value.text === undefined
        ? yield* remote.body(value.hash)
        : Stream.succeed(new TextEncoder().encode(value.text));

    const captured = yield* cacheBody(root, content);

    if (captured.hash !== value.hash || captured.size !== value.size)
      return yield* new LocalFailure({ message: `Content hash or size mismatch: ${value.path}` });

    return captured;
  });

  const observe = Effect.fn("Sync.observe")(function* (folderId: string, file: string) {
    if (ignoredPath(file)) return;
    const normalized = yield* normalisePath(file);
    const local = yield* captureLocal(root, filePath(folderId, normalized));
    const hash = local?.hash ?? null;
    const known = yield* state.head(folderId, normalized);
    const pending = yield* hasPending(folderId, normalized);

    if (isEcho(hash, known, pending)) return;

    if (!writable(folderId)) return;
    yield* state.enqueue(
      {
        folderId,
        path: normalized,
        txid: yield* crypto.randomUUIDv4(),
        base: conflictBase(known),
        hash,
        size: local?.size ?? 0,
        source: local?.source ?? null,
        uploaded: 0,
      },
      {
        folderId,
        path: normalized,
        version: known?.version ?? null,
        hash: known?.hash ?? null,
        localHash: hash,
        localVersion: known?.localVersion ?? null,
      },
    );
  }, skip);

  const install = Effect.fn("Sync.install")(function* (
    folderId: string,
    file: string,
    value: File | null,
    bootstrap = false,
  ) {
    const known = yield* state.head(folderId, file);
    const remoteHash = value?.hash ?? null;
    const pending = yield* hasPending(folderId, file);

    if (pending) {
      if (known !== undefined)
        yield* state.save({ ...known, version: value?.version ?? null, hash: remoteHash });

      return;
    }

    // Download before observing the working file: editors keep writing while a blob read waits.
    const content = value === null ? null : yield* cache(value);
    const local = yield* captureLocal(root, filePath(folderId, file));
    const hash = local?.hash ?? null;

    if (local !== null && hash !== remoteHash && !isEcho(hash, known)) {
      if (writable(folderId)) {
        if (!bootstrap && known !== undefined) {
          yield* observe(folderId, file);
          yield* state.save({
            ...known,
            version: value?.version ?? null,
            hash: remoteHash,
            localHash: hash,
          });

          return;
        }

        let copy = conflictPath(file, clientId);
        let suffix = 0;

        while (yield* fs.exists(path.join(root, filePath(folderId, copy))))
          copy = conflictPath(file, clientId, ++suffix);
        yield* replaceSource(root, filePath(folderId, copy), local.source);
        yield* state.enqueue(
          {
            folderId,
            path: copy,
            txid: yield* crypto.randomUUIDv4(),
            base: null,
            ...local,
            uploaded: 0,
          },
          { folderId, path: copy, version: null, hash: null, localHash: hash, localVersion: null },
        );
        yield* Effect.logWarning(`Preserved bootstrap conflict: ${folderId}/${copy}`);
      } else {
        const base = path.join(
          ".folder-sync",
          "rejected",
          roots.get(folderId) ?? mapping(folderId),
          file,
        );

        let backup = base;
        let suffix = 0;

        while (yield* fs.exists(path.join(root, backup))) backup = backupPath(base, ++suffix);
        yield* replaceSource(root, backup, local.source);
        yield* Effect.logWarning(`Preserved read-only edit: ${backup}`);
      }
    }

    const row: Known = {
      folderId,
      path: file,
      version: value?.version ?? null,
      hash: remoteHash,
      localHash: known?.localHash ?? null,
      localVersion: known?.localVersion ?? null,
    };

    yield* state.save(row);
    yield* replaceSource(root, filePath(folderId, file), content?.source ?? null);
    yield* state.save({ ...row, localHash: remoteHash, localVersion: row.version });
  }, skip);

  const guardFolder = Effect.fn("Sync.guardFolder")(function* (folderId: string) {
    const saved = yield* state.get(`cursor:${folderId}`);
    const tail = yield* remote.tail(folderId);

    if (saved !== undefined && historyRewound(saved, tail))
      return yield* new HistoryRewound({
        message: `Server history went backwards: ${folderId} cursor ${saved} is past tail ${tail}. Sync stopped. Restore the server history or use a new --dir.`,
      });

    return saved;
  });

  const readReplacement = Effect.fn("Sync.readReplacement")(function* (
    folderId: string,
    from: string,
    first?: Effect.Success<ReturnType<typeof remote.page>>,
  ) {
    const files = new Map<string, File>();
    let cursor = from;
    let pending = first;

    while (true) {
      const page = pending ?? (yield* remote.page(folderId, cursor, false));
      pending = undefined;

      if (page.reset) {
        if (cursor === "-1")
          return yield* new RemoteFailure({
            message: `Folder history is unavailable from the beginning: ${folderId}`,
            retryable: false,
            status: 410,
          });
        files.clear();
        cursor = "-1";
        continue;
      }

      applyChanges(files, page.rows);

      if (page.cursor === cursor && !page.upToDate)
        return yield* new RemoteFailure({
          message: `Folder read did not advance: ${folderId}`,
          retryable: false,
          status: 0,
        });

      cursor = page.cursor;

      if (page.upToDate) break;
    }

    return { files, cursor };
  });

  const initialiseFolder = Effect.fn("Sync.initialiseFolder")(function* (folderId: string) {
    const saved = yield* guardFolder(folderId);

    if (saved !== undefined) return yield* Effect.void;
    const { files, cursor } = yield* readReplacement(folderId, "-1");

    yield* gate.withPermit(
      Effect.gen(function* () {
        const paths = new Set([
          ...files.keys(),
          ...(yield* state.heads())
            .filter((row) => row.folderId === folderId)
            .map((row) => row.path),
          ...(yield* localFiles(path.join(root, roots.get(folderId) ?? mapping(folderId)), warn)),
        ]);

        for (const file of paths) {
          if (ignoredPath(file)) continue;

          // A brand-new local path is a create, not a deletion discovered during replay.
          if (!files.has(file) && (yield* state.head(folderId, file)) === undefined)
            yield* observe(folderId, file);
          else yield* install(folderId, file, files.get(file) ?? null, true);
        }

        yield* state.set(`cursor:${folderId}`, cursor);
      }),
    );

    return yield* Effect.void;
  });

  const applyPage = Effect.fn("Sync.applyPage")(function* (
    folderId: string,
    rows: ReadonlyArray<FileMessage>,
    cursor: string,
  ) {
    yield* gate.withPermit(
      Effect.gen(function* () {
        for (const change of rows) {
          if (isReset(change) || ignoredPath(change.key)) continue;
          yield* observe(folderId, change.key);
          yield* install(folderId, change.key, isUpsert(change) ? change.value : null);
        }

        yield* state.set(`cursor:${folderId}`, cursor);
      }),
    );
  });

  const pullFolder = Effect.fn("Sync.pullFolder")(function* (folderId: string, once = false) {
    let cursor = (yield* state.get(`cursor:${folderId}`)) ?? "-1";
    let live = false;

    while (true) {
      const page: Effect.Success<ReturnType<typeof remote.page>> = yield* remote.page(
        folderId,
        cursor,
        live,
      );

      if (page.reset || page.rows.some(isReset)) {
        // A stored reset replaces state at this position. Only 410 reads from -1.
        // Finish the replacement pages before reconciling paths absent from the new state.
        const { files, cursor: replay } = yield* readReplacement(folderId, cursor, page);

        yield* gate.withPermit(
          Effect.gen(function* () {
            const paths = new Set([
              ...files.keys(),
              ...(yield* state.heads())
                .filter((row) => row.folderId === folderId)
                .map((row) => row.path),
            ]);

            for (const file of paths) yield* install(folderId, file, files.get(file) ?? null, true);
            yield* state.set(`cursor:${folderId}`, replay);
          }),
        );
        cursor = replay;
        live = false;

        if (once) return;
        continue;
      }

      yield* applyPage(folderId, page.rows, page.cursor);
      cursor = page.cursor;
      live = page.upToDate;

      if (once && page.upToDate) return;
    }
  });

  const followers = yield* FiberSet.make<void, Effect.Error<ReturnType<typeof pullFolder>>>();
  const running = new Set<string>();

  const addFolder = Effect.fn("Sync.addFolder")(function* (folderId: string) {
    if (!selected(folderId) || running.has(folderId)) return yield* Effect.void;

    const relative = yield* folderRoot(folderId, mapping(folderId), roots).pipe(
      Effect.catchTag("InvalidFile", ({ message }) => new LocalFailure({ message })),
    );

    const previous = yield* state.get(`mapping:${folderId}`);

    if (previous !== undefined && previous !== relative)
      return yield* new LocalFailure({
        message: `Folder mapping changed for ${folderId}. Use another directory.`,
      });
    roots.set(folderId, relative);
    yield* state.set(`mapping:${folderId}`, relative);
    yield* state.addFolder(folderId);
    yield* safePath(root, relative);
    yield* fs.makeDirectory(path.join(root, relative), { recursive: true });
    yield* initialiseFolder(folderId);
    yield* pullFolder(folderId, true);
    running.add(folderId);
    yield* FiberSet.run(followers, retryRemote(pullFolder(folderId)));

    return yield* Effect.void;
  });

  const discover = Effect.fn("Sync.discover")(function* (live: boolean) {
    const offset = (yield* state.get("foldersCursor")) ?? "-1";
    const page = yield* remote.folders(offset, live);

    if (page.reset) {
      if (offset === "-1")
        return yield* new RemoteFailure({
          message: "Folder registry history is unavailable from the beginning",
          retryable: false,
          status: 410,
        });
      yield* state.set("foldersCursor", "-1");

      return false;
    }

    // Folder lifecycle changes are out of scope. Keep configured mirrors, and discover
    // registrations after the last reset without re-reading that stored control.
    for (const change of afterLastReset(page.rows))
      if (isUpsert(change)) yield* addFolder(change.value.folderId);
    yield* state.set("foldersCursor", page.cursor);

    return page.upToDate;
  });

  const scan = Effect.fn("Sync.scan")(function* () {
    for (const [folderId, relative] of roots) {
      const files = new Set([
        ...(yield* localFiles(path.join(root, relative), warn)),
        ...(yield* state.heads()).filter((row) => row.folderId === folderId).map((row) => row.path),
      ]);

      for (const file of files) {
        if (writable(folderId)) yield* observe(folderId, file);
        else {
          const known = yield* state.head(folderId, file);

          const local = yield* captureLocal(root, filePath(folderId, file)).pipe(
            Effect.catchIf(Schema.is(SkippedLocal), (error) =>
              warn(error.path, error.message).pipe(Effect.as(null)),
            ),
          );

          if (!isEcho(local?.hash ?? null, known)) {
            const head = yield* remote.head(folderId, file);

            if (head === null) yield* install(folderId, file, null);
            else {
              const content = yield* cacheBody(root, head.body);
              yield* install(folderId, file, {
                path: file,
                version: head.version,
                hash: content.hash,
                size: content.size,
                author: "server",
              });
            }
          }
        }
      }
    }

    yield* state.set("lastScan", String(yield* Clock.currentTimeMillis));
  });

  const refresh = Effect.fn("Sync.refresh")(function* (folderId: string, file: string) {
    const head = yield* remote.head(folderId, file);

    if (head === null) return yield* install(folderId, file, null);
    const content = yield* cacheBody(root, head.body);

    return yield* install(folderId, file, {
      path: file,
      version: head.version,
      hash: content.hash,
      size: content.size,
      author: "server",
    });
  });

  // Check every saved folder before installing any file from a resumed mirror.
  const savedFolders = (yield* state.folders()).filter(selected);

  for (const folderId of savedFolders) yield* guardFolder(folderId);

  for (const folderId of savedFolders) yield* addFolder(folderId);

  while (!(yield* discover(false))) {
    /* Drain the registry before declaring this mirror ready. */
  }

  for (const folderId of [...(options.folders ?? []), ...options.writable])
    yield* addFolder(folderId);

  yield* gate.withPermit(
    Effect.gen(function* () {
      for (const row of yield* state.heads()) {
        if (
          !selected(row.folderId) ||
          row.localVersion === row.version ||
          (yield* hasPending(row.folderId, row.path))
        )
          continue;
        // An interrupted install or conflict refresh left the disk on an older observation.
        // Capture edits made while stopped before replacing anything or advancing their base.
        yield* observe(row.folderId, row.path);
        yield* refresh(row.folderId, row.path);
      }
    }),
  );
  yield* state.set("ready", "true");

  const deliver = Effect.fn("Sync.deliver")(function* () {
    const operation = (yield* state.pending())[0];

    if (operation === undefined) return;

    if (operation.source === null && operation.base === null) {
      // No observed remote version belongs to this local delete.
      yield* gate.withPermit(
        state
          .discard(operation.seq)
          .pipe(Effect.andThen(refresh(operation.folderId, operation.path))),
      );

      return;
    }

    // Conflict intent was saved before its disk copy. Complete it after a restart as well.
    const restore = yield* state.get(`restore:${operation.seq}`);

    if (restore !== undefined && restore !== "") {
      yield* gate.withPermit(
        Effect.gen(function* () {
          const current = yield* captureLocal(root, filePath(operation.folderId, operation.path));
          const known = yield* state.head(operation.folderId, operation.path);

          if (current === null && known === undefined) {
            yield* replaceSource(
              root,
              filePath(operation.folderId, operation.path),
              operation.source,
            );
            yield* state.save({
              folderId: operation.folderId,
              path: operation.path,
              version: null,
              hash: null,
              localHash: operation.hash,
              localVersion: null,
            });
          } else if ((current?.hash ?? null) !== operation.hash) {
            // A user may edit or remove the copy while this daemon is down. Queue that observation first.
            yield* observe(operation.folderId, operation.path);
          }

          yield* state.set(`restore:${operation.seq}`, "");
        }),
      );
    }

    const needsBlob =
      operation.source !== null &&
      (operation.size > INLINE_LIMIT ||
        inlineText(yield* fs.readFile(operation.source)) === undefined);

    if (needsBlob && operation.uploaded === 0) {
      yield* remote.upload(operation);
      yield* state.uploaded(operation.seq);
    }

    const result = yield* remote.push(operation).pipe(
      Effect.catchTag("SyncBlobMissing", () =>
        remote.upload(operation).pipe(Effect.andThen(remote.push(operation))),
      ),
      Effect.catchTags({
        StaleFile: () => Effect.succeed(null),
        RemoteFailure: (error) =>
          Effect.gen(function* () {
            if (error.retryable || [401, 403].includes(error.status)) return yield* error;
            yield* state.reject(operation.seq, error.status, error.message);
            yield* Effect.logWarning(
              `Rejected write retained in local cache: ${operation.folderId}/${operation.path} (${error.status})`,
            );

            return undefined;
          }),
      }),
    );

    if (result === undefined) return;
    yield* gate.withPermit(
      Effect.gen(function* () {
        yield* observe(operation.folderId, operation.path);

        if (result === null && operation.source !== null) {
          let suffix = 0;
          let copy = conflictPath(operation.path, clientId);
          const pending = yield* state.pending();

          while (
            (yield* fs.exists(path.join(root, filePath(operation.folderId, copy)))) ||
            pending.some((op) => op.folderId === operation.folderId && op.path === copy)
          )
            copy = conflictPath(operation.path, clientId, ++suffix);
          yield* state.conflict(operation.seq, copy, yield* crypto.randomUUIDv4());
          yield* replaceSource(root, filePath(operation.folderId, copy), operation.source);
          yield* state.save({
            folderId: operation.folderId,
            path: copy,
            version: null,
            hash: null,
            localHash: operation.hash,
            localVersion: null,
          });
          yield* state.set(`restore:${operation.seq}`, "");
          yield* Effect.logWarning(`Preserved conflict: ${operation.folderId}/${copy}`);
        } else if (result === null) yield* state.discard(operation.seq);
        else yield* state.settle(operation, result.version ?? null);
        yield* refresh(operation.folderId, operation.path);
      }),
    );
  });

  let lastEvent = 0;
  const settle = options.settleMillis ?? 100;

  const scanSafely = Effect.gen(function* () {
    if ((yield* Clock.currentTimeMillis) - lastEvent >= settle) yield* gate.withPermit(scan());
  });

  const watch = fs.watch(root, { recursive: true }).pipe(
    Stream.filter(
      (event) => event.path.split(path.sep)[0] !== ".folder-sync" && !ignoredPath(event.path),
    ),
    Stream.tap(() =>
      Clock.currentTimeMillis.pipe(
        Effect.tap((now) =>
          Effect.sync(() => {
            lastEvent = now;
          }),
        ),
      ),
    ),
    Stream.debounce(settle),
    Stream.runForEach(() => scanSafely),
  );

  yield* Effect.all(
    [
      FiberSet.join(followers),
      watch,
      retryRemote(scanSafely).pipe(Effect.repeat(Schedule.spaced("1 second"))),
      retryRemote(discover(true)).pipe(Effect.repeat(Schedule.forever)),
      retryRemote(deliver()).pipe(Effect.repeat(Schedule.spaced("100 millis"))),
    ],
    { concurrency: "unbounded", discard: true },
  );

  return yield* Effect.void;
});

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

      return stateLayer(path.join(root, ".folder-sync/state.sqlite"));
    }),
  ).pipe(Layer.provide(BunServices.layer));

  const remote = remoteLayer(options.server, options.token).pipe(
    Layer.provide([FetchHttpClient.layer, BunServices.layer]),
  );

  return runSync(options).pipe(
    Effect.scoped,
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Exported executable boundary owns the scoped CLI runtime.
    Effect.provide(Layer.mergeAll(state, remote, BunServices.layer, BunCrypto.layer)),
  );
};
