import { BunCrypto, BunServices } from "@effect/platform-bun";
import {
  Array,
  Clock,
  Duration,
  Effect,
  FiberSet,
  FileSystem,
  Layer,
  Path,
  Predicate,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient } from "effect/http";
import { afterLastReset, isUpsert } from "../server/messages.ts";
import { RemoteFailure, remoteLayer } from "./client.ts";
import { openContext, mappedDirectory, selected, type SyncContext } from "./context.ts";
import { ignoredPath, type SyncOptions } from "./decisions.ts";
import { installer } from "./install.ts";
import { folderRoot } from "./layout.ts";
import { LocalFailure, pathKind, safePath } from "./local.ts";
import { puller } from "./pull.ts";
import { pusher } from "./push.ts";
import { scanner } from "./scan.ts";
import { stateLayer } from "./state.ts";

/** Retry a remote read or write while it fails with a retryable status or a transport error. */
const retryRemote = <A, E, R>(work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.retry({
      while: (error) =>
        (Schema.is(RemoteFailure)(error) && error.retryable) ||
        Predicate.isTagged(error, "HttpClientError"),
      schedule: Schedule.exponential("100 millis").pipe(
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Math.min(Duration.toMillis(duration), 3000)),
        ),
      ),
    }),
  );

/** Registry discovery and the start of one follower for each selected folder. */
const folderDiscovery = Effect.fn("Sync.folders")(function* (
  context: SyncContext,
  pull: ReturnType<typeof puller>,
) {
  const { fs, path, state, remote, root, roots } = context;
  const followers = yield* FiberSet.make<void, Effect.Error<ReturnType<typeof pull.pullFolder>>>();
  const running = new Set<string>();

  const addFolder = Effect.fn("Sync.addFolder")(function* (folderId: string) {
    if (!selected(context, folderId) || running.has(folderId)) return yield* Effect.void;

    const relative = yield* folderRoot(folderId, mappedDirectory(context, folderId), roots).pipe(
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
    yield* pull.initialiseFolder(folderId);
    yield* pull.pullFolder(folderId, true);
    running.add(folderId);
    yield* FiberSet.run(followers, retryRemote(pull.pullFolder(folderId)));

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

  return { addFolder, discover, followers };
});

/** Run one mirror: resume, discover, catch up, then follow, watch, scan and deliver. */
export const runSync = Effect.fn("FolderSync.run")(function* (options: SyncOptions) {
  const context = yield* openContext(options);
  const { fs, path, state, root, gate } = context;
  const install = installer(context);
  const pull = puller(context, install);
  const { deliver } = pusher(context, install);
  const { scan } = scanner(context, install);
  const { addFolder, discover, followers } = yield* folderDiscovery(context, pull);

  // Check every saved folder before installing any file from a resumed mirror.
  const savedFolders = Array.filter(yield* state.folders(), (id) => selected(context, id));

  for (const folderId of savedFolders) yield* pull.guardFolder(folderId);

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
          !selected(context, row.folderId) ||
          row.localVersion === row.version ||
          (yield* state.hasPending(row.folderId, row.path))
        )
          continue;
        // An interrupted install or refresh left the disk on an older observation.
        // Capture edits made while stopped before replacing anything or advancing their base.
        yield* install.observe(row.folderId, row.path);
        yield* install.refresh(row.folderId, row.path);
      }
    }),
  );
  yield* state.set("ready", "true");

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
