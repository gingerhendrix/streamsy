import { Effect } from "effect";
import { isTombstone, normalisePath, type File, type FileValue } from "../server/domain.ts";
import { filePath, folderPath, freePath, skip, writable, type SyncContext } from "./context.ts";
import { backupPath, editBase, ignoredPath, isEcho } from "./decisions.ts";
import { cacheBody, captureLocal, LocalFailure, replaceSource, safePath } from "./local.ts";
import type { Known } from "./state.ts";

/**
 * Local observation and remote installation for one sync run. `observe` queues
 * a local edit. `install` puts a remote value on disk and queues any local edit
 * that it would replace. `refresh` reads the current head from the server and
 * installs it. Callers hold the gate.
 *
 * A known row keeps the version of the last change of the path, a tombstone too.
 * That version is the parent of the next local edit.
 */
export const installer = (context: SyncContext) => {
  const { fs, path, crypto, state, remote, root } = context;
  const exists = (file: string) => fs.exists(path.join(root, file));

  const cache = Effect.fn("Sync.cache")(function* (value: File) {
    const source = yield* safePath(root, `.folder-sync/content/${value.hash}`);

    if (yield* fs.exists(source)) return { source, hash: value.hash, size: value.size };

    const captured = yield* cacheBody(root, yield* remote.body(value.hash));

    if (captured.hash !== value.hash || captured.size !== value.size)
      return yield* new LocalFailure({ message: `Content hash or size mismatch: ${value.path}` });

    return captured;
  });

  /** Queue the local state of a path as a write based on `base`. */
  const observe = Effect.fn("Sync.observe")(function* (folderId: string, file: string) {
    if (ignoredPath(file)) return;
    const normalized = yield* normalisePath(file);
    const local = yield* captureLocal(root, filePath(context, folderId, normalized));
    const hash = local?.hash ?? null;
    const known = yield* state.head(folderId, normalized);
    const pending = yield* state.hasPending(folderId, normalized);

    if (isEcho(hash, known, pending)) return;

    if (!writable(context, folderId)) return;
    yield* state.enqueue(
      {
        folderId,
        path: normalized,
        txid: yield* crypto.randomUUIDv4(),
        base: editBase(known),
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
  }, skip(context));

  /**
   * A first sync found a local file that differs from the server. The local file
   * is committed with parent `null`: it becomes the head, and the server copy
   * stays in history. A fork detector sees a fork from "no file".
   */
  const keepFirstSyncEdit = Effect.fn("Sync.keepFirstSyncEdit")(function* (
    folderId: string,
    file: string,
    local: { readonly source: string; readonly hash: string; readonly size: number },
    value: FileValue | null,
  ) {
    yield* state.enqueue(
      {
        folderId,
        path: file,
        txid: yield* crypto.randomUUIDv4(),
        base: null,
        ...local,
        uploaded: 0,
      },
      {
        folderId,
        path: file,
        version: value?.version ?? null,
        hash: liveHash(value),
        localHash: local.hash,
        localVersion: null,
      },
    );
    yield* Effect.logWarning(`First sync keeps the local file as the head: ${folderId}/${file}`);
  });

  /** A read-only mirror cannot upload an edit. Move it under `.folder-sync/rejected`. */
  const keepReadOnlyEdit = Effect.fn("Sync.keepReadOnlyEdit")(function* (
    folderId: string,
    file: string,
    source: string,
  ) {
    const base = path.join(".folder-sync", "rejected", folderPath(context, folderId), file);
    const backup = yield* freePath((suffix) => backupPath(base, suffix), exists);

    yield* replaceSource(root, backup, source);
    yield* Effect.logWarning(`Preserved read-only edit: ${backup}`);
  });

  const install = Effect.fn("Sync.install")(function* (
    folderId: string,
    file: string,
    value: FileValue | null,
  ) {
    const known = yield* state.head(folderId, file);
    const remoteHash = liveHash(value);
    const version = value?.version ?? null;

    if (yield* state.hasPending(folderId, file)) {
      if (known !== undefined) yield* state.save({ ...known, version, hash: remoteHash });

      return;
    }

    // Download before observing the working file: editors keep writing while a blob read waits.
    const live = value === null || isTombstone(value) ? null : value;
    const content = live === null ? null : yield* cache(live);
    const local = yield* captureLocal(root, filePath(context, folderId, file));
    const hash = local?.hash ?? null;

    if (local !== null && hash !== remoteHash && !isEcho(hash, known)) {
      if (!writable(context, folderId)) yield* keepReadOnlyEdit(folderId, file, local.source);
      else if (known === undefined) {
        // The local file stays on disk and becomes the head when the outbox sends it.
        yield* keepFirstSyncEdit(folderId, file, local, value);

        return;
      } else {
        // A local edit that the mirror has not queued yet. It is sent later and becomes the head.
        yield* observe(folderId, file);
        yield* state.save({ ...known, version, hash: remoteHash, localHash: hash });

        return;
      }
    }

    const row: Known = {
      folderId,
      path: file,
      version,
      hash: remoteHash,
      localHash: known?.localHash ?? null,
      localVersion: known?.localVersion ?? null,
    };

    yield* state.save(row);
    yield* replaceSource(root, filePath(context, folderId, file), content?.source ?? null);
    yield* state.save({ ...row, localHash: remoteHash, localVersion: row.version });
  }, skip(context));

  /**
   * Install the server's current head of one path. A deleted path installs its
   * tombstone version, and a path that never existed installs nothing.
   */
  const refresh = Effect.fn("Sync.refresh")(function* (folderId: string, file: string) {
    const head = yield* remote.head(folderId, file);

    if (head === null) return yield* install(folderId, file, null);

    if ("deleted" in head)
      return yield* install(folderId, file, {
        path: file,
        version: head.deleted,
        parent: null,
        author: "server",
        deleted: true,
      });
    const content = yield* cacheBody(root, head.body);

    // `install` reads only the version and the content. The parent and author are not sent here.
    return yield* install(folderId, file, {
      path: file,
      version: head.version,
      parent: null,
      hash: content.hash,
      size: content.size,
      author: "server",
    });
  });

  return { observe, install, refresh };
};

export type Install = ReturnType<typeof installer>;

/** The content hash of a live value. A tombstone and a missing path have none. */
const liveHash = (value: FileValue | null) =>
  value === null || isTombstone(value) ? null : value.hash;
