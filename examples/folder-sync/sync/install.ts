import { Effect } from "effect";
import { normalisePath, type File } from "../server/domain.ts";
import { filePath, folderPath, freePath, skip, writable, type SyncContext } from "./context.ts";
import { backupPath, conflictBase, conflictPath, ignoredPath, isEcho } from "./decisions.ts";
import { cacheBody, captureLocal, LocalFailure, replaceSource, safePath } from "./local.ts";
import type { Known } from "./state.ts";

/**
 * Local observation and remote installation for one sync run. `observe` queues
 * a local edit. `install` puts a remote value on disk and keeps any local edit
 * that it would replace. `refresh` reads the current head from the server and
 * installs it. Callers hold the gate.
 */
export const installer = (context: SyncContext) => {
  const { fs, path, crypto, state, remote, root, clientId } = context;
  const exists = (file: string) => fs.exists(path.join(root, file));

  const cache = Effect.fn("Sync.cache")(function* (value: File) {
    const source = yield* safePath(root, `.folder-sync/content/${value.hash}`);

    if (yield* fs.exists(source)) return { source, hash: value.hash, size: value.size };

    const captured = yield* cacheBody(root, yield* remote.body(value.hash));

    if (captured.hash !== value.hash || captured.size !== value.size)
      return yield* new LocalFailure({ message: `Content hash or size mismatch: ${value.path}` });

    return captured;
  });

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
  }, skip(context));

  /** A first sync found a local file that differs from the server. Keep it as a new conflict file. */
  const keepBootstrapConflict = Effect.fn("Sync.keepBootstrapConflict")(function* (
    folderId: string,
    file: string,
    local: { readonly source: string; readonly hash: string; readonly size: number },
  ) {
    const copy = yield* freePath(
      (suffix) => conflictPath(file, clientId, suffix),
      (candidate) => exists(filePath(context, folderId, candidate)),
    );

    yield* replaceSource(root, filePath(context, folderId, copy), local.source);
    yield* state.enqueue(
      {
        folderId,
        path: copy,
        txid: yield* crypto.randomUUIDv4(),
        base: null,
        ...local,
        uploaded: 0,
      },
      {
        folderId,
        path: copy,
        version: null,
        hash: null,
        localHash: local.hash,
        localVersion: null,
      },
    );
    yield* Effect.logWarning(`Preserved bootstrap conflict: ${folderId}/${copy}`);
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
    value: File | null,
    bootstrap = false,
  ) {
    const known = yield* state.head(folderId, file);
    const remoteHash = value?.hash ?? null;

    if (yield* state.hasPending(folderId, file)) {
      if (known !== undefined)
        yield* state.save({ ...known, version: value?.version ?? null, hash: remoteHash });

      return;
    }

    // Download before observing the working file: editors keep writing while a blob read waits.
    const content = value === null ? null : yield* cache(value);
    const local = yield* captureLocal(root, filePath(context, folderId, file));
    const hash = local?.hash ?? null;

    if (local !== null && hash !== remoteHash && !isEcho(hash, known)) {
      if (!writable(context, folderId)) yield* keepReadOnlyEdit(folderId, file, local.source);
      else if (bootstrap || known === undefined)
        yield* keepBootstrapConflict(folderId, file, local);
      else {
        yield* observe(folderId, file);
        yield* state.save({
          ...known,
          version: value?.version ?? null,
          hash: remoteHash,
          localHash: hash,
        });

        return;
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
    yield* replaceSource(root, filePath(context, folderId, file), content?.source ?? null);
    yield* state.save({ ...row, localHash: remoteHash, localVersion: row.version });
  }, skip(context));

  /** Install the server's current head of one path, or remove the path when the server has none. */
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

  return { observe, install, refresh };
};

export type Install = ReturnType<typeof installer>;
