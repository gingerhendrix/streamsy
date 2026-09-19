import { Context, Effect, Layer } from "effect";
import { Storage } from "../storage/storage.ts";
import { StreamsReader, StreamsWriter } from "./tags.ts";
import { create } from "./create.ts";
import { append } from "./append.ts";
import { head, read, readNext } from "./read.ts";
import { StreamNotFound, StreamGone, type RemoveError } from "./errors.ts";
import { expireIfNeeded } from "./expiry.ts";
export { expireDue } from "./expiry.ts";
export { create } from "./create.ts";
export { expireIfNeeded } from "./expiry.ts";

export interface ProtocolOptions {
  /** Maximum messages per catch-up page; live reads return the whole tail. Default 1000. */
  readonly readLimit?: number;
  readonly longPollTimeoutMs?: number;
}
export const layer = (
  options: ProtocolOptions = {},
): Layer.Layer<StreamsReader | StreamsWriter, never, Storage> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const storage = yield* Storage;
      const timeout = options.longPollTimeoutMs ?? 30_000;
      if (!Number.isFinite(timeout) || timeout <= 0)
        return yield* Effect.die(new RangeError("longPollTimeoutMs must be positive"));
      const readLimit = options.readLimit ?? 1000;
      if (!Number.isSafeInteger(readLimit) || readLimit <= 0)
        return yield* Effect.die(new RangeError("readLimit must be a positive safe integer"));
      const reader = StreamsReader.of({
        head: (id) => head(storage, id),
        read: (id, opts) => read(storage, id, opts, readLimit),
        readNext: (id, opts) => readNext(storage, id, opts, timeout),
      });
      const writer = StreamsWriter.of({
        create: (id, opts) => create(storage, id, opts),
        fork: (id, source, opts) => create(storage, id, { ...opts, forkedFrom: source }),
        append: (id, opts) => append(storage, id, opts),
        remove: Effect.fn("Protocol.remove")(function* (id): Effect.fn.Return<
          void,
          RemoveError | import("../fault.ts").StorageFault
        > {
          yield* expireIfNeeded(storage, id);
          yield* storage
            .mutate({ operations: [{ _tag: "Delete", streamId: id, reason: "delete" }] })
            .pipe(
              Effect.uninterruptible,
              Effect.catchTag(
                "MutationRejected",
                (rejection): Effect.Effect<never, RemoveError> => {
                  if (rejection.reason === "not-found")
                    return Effect.fail(new StreamNotFound({ id }));
                  if (rejection.reason === "gone") return Effect.fail(new StreamGone({ id }));
                  return Effect.die(new Error(`Unexpected delete rejection: ${rejection.reason}`));
                },
              ),
            );
        }),
      });
      return Context.make(StreamsReader, reader).pipe(Context.add(StreamsWriter, writer));
    }),
  );
