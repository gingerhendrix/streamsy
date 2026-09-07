import { Context, Effect, Predicate, Layer } from "effect";
import { Storage } from "../storage/storage.ts";
import { StreamsReader, StreamsWriter } from "./tags.ts";
import { create } from "./create.ts";
import { append } from "./append.ts";
import { head, read, readNext } from "./read.ts";
import type { RemoveOutcome } from "./outcomes.ts";
import { expireIfNeeded } from "./expiry.ts";
export { expireDue } from "./expiry.ts";
export { create } from "./create.ts";
export { expireIfNeeded } from "./expiry.ts";

export interface ProtocolOptions {
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
      const reader = StreamsReader.of({
        head: (id) => head(storage, id),
        read: (id, opts) => read(storage, id, opts),
        readNext: (id, opts) => readNext(storage, id, opts, timeout),
      });
      const writer = StreamsWriter.of({
        create: (id, opts) => create(storage, id, opts),
        fork: (id, source, opts) => create(storage, id, { ...opts, forkedFrom: source }),
        append: (id, opts) => append(storage, id, opts),
        remove: Effect.fn("Protocol.remove")(function* (id): Effect.fn.Return<
          RemoveOutcome,
          import("../fault.ts").StorageFault
        > {
          yield* expireIfNeeded(storage, id);
          const outcome = yield* storage
            .mutate({ operations: [{ _tag: "Delete", streamId: id, reason: "delete" }] })
            .pipe(Effect.uninterruptible);
          if (Predicate.isTagged(outcome, "Applied")) return { status: "ok" };
          if (outcome.reason === "not-found" || outcome.reason === "gone")
            return { status: outcome.reason };
          return yield* Effect.die(new Error(`Unexpected delete rejection: ${outcome.reason}`));
        }),
      });
      return Context.make(StreamsReader, reader).pipe(Context.add(StreamsWriter, writer));
    }),
  );
