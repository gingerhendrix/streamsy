import { Effect } from "effect";
import { filePath, freePath, type SyncContext } from "./context.ts";
import { conflictPath } from "./decisions.ts";
import type { Install } from "./install.ts";
import { captureLocal, replaceSource } from "./local.ts";
import type { Pending } from "./state.ts";

/** Delivery of the outbox for one sync run, one operation at a time, oldest first. */
export const pusher = (context: SyncContext, { observe, refresh }: Install) => {
  const { fs, path, crypto, state, remote, root, gate, clientId } = context;

  /** Conflict intent was saved before its disk copy. Complete it after a restart as well. */
  const restoreConflictCopy = Effect.fn("Sync.restoreConflictCopy")(function* (operation: Pending) {
    const current = yield* captureLocal(
      root,
      filePath(context, operation.folderId, operation.path),
    );

    const known = yield* state.head(operation.folderId, operation.path);

    if (current === null && known === undefined) {
      yield* replaceSource(
        root,
        filePath(context, operation.folderId, operation.path),
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
  });

  /** The server has a newer version. Move this write to a free conflict file and keep it queued. */
  const moveToConflictCopy = Effect.fn("Sync.moveToConflictCopy")(function* (
    operation: Pending & { readonly source: string },
  ) {
    const copy = yield* freePath(
      (suffix) => conflictPath(operation.path, clientId, suffix),
      (candidate) =>
        Effect.gen(function* () {
          return (
            (yield* fs.exists(path.join(root, filePath(context, operation.folderId, candidate)))) ||
            (yield* state.hasPending(operation.folderId, candidate))
          );
        }),
    );

    yield* state.conflict(operation.seq, copy, yield* crypto.randomUUIDv4());
    yield* replaceSource(root, filePath(context, operation.folderId, copy), operation.source);
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
  });

  /** Upload the content as a blob, then commit with a precondition. A delete has no content. */
  const send = Effect.fn("Sync.send")(function* (operation: Pending) {
    if (operation.source !== null && operation.uploaded === 0) {
      yield* remote.upload(operation);
      yield* state.uploaded(operation.seq);
    }

    return yield* remote.push(operation).pipe(
      Effect.catchTag("SyncBlobMissing", () =>
        remote.upload(operation).pipe(Effect.andThen(remote.push(operation))),
      ),
      Effect.catchTags({
        // `null` is a stale write: the server holds another version.
        StaleFile: () => Effect.succeed(null),
        RemoteFailure: (error) =>
          Effect.gen(function* () {
            if (error.retryable || [401, 403].includes(error.status)) return yield* error;
            yield* state.reject(operation.seq, error.status, error.message);
            yield* Effect.logWarning(
              `Rejected write retained in local cache: ${operation.folderId}/${operation.path} (${error.status})`,
            );

            // `undefined` is a rejected write. It left the outbox.
            return undefined;
          }),
      }),
    );
  });

  const deliver = Effect.fn("Sync.deliver")(function* () {
    const operation = yield* state.next();

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

    const restore = yield* state.get(`restore:${operation.seq}`);

    if (restore !== undefined && restore !== "")
      yield* gate.withPermit(restoreConflictCopy(operation));

    const result = yield* send(operation);

    if (result === undefined) return;
    yield* gate.withPermit(
      Effect.gen(function* () {
        yield* observe(operation.folderId, operation.path);

        if (result === null && operation.source !== null)
          yield* moveToConflictCopy({ ...operation, source: operation.source });
        else if (result === null) yield* state.discard(operation.seq);
        else yield* state.settle(operation, result.version ?? null);
        yield* refresh(operation.folderId, operation.path);
      }),
    );
  });

  return { deliver };
};
