import { Effect } from "effect";
import type { SyncContext } from "./context.ts";
import type { Install } from "./install.ts";
import type { Pending } from "./state.ts";

/** Delivery of the outbox for one sync run, one operation at a time, oldest first. */
export const pusher = (context: SyncContext, { observe, refresh }: Install) => {
  const { state, remote, gate } = context;

  /**
   * Upload the content as a blob, then commit with the edit's parent. A delete has
   * no content. Every accepted write lands, so there is no stale answer.
   */
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

    const result = yield* send(operation);

    if (result === undefined) return;
    yield* gate.withPermit(
      Effect.gen(function* () {
        yield* observe(operation.folderId, operation.path);
        yield* state.settle(operation, result.version ?? null);
        yield* refresh(operation.folderId, operation.path);
      }),
    );
  });

  return { deliver };
};
