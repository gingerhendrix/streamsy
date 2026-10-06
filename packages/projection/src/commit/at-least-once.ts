/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord is a Schema.Struct plain data record. */
import { Effect } from "effect";
import { make } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { beforeRecord, detailsOf, load, remove, pin } from "./internal.ts";

export const atLeastOnce = pin(
  make<void, never, CheckpointStore>({
    name: "atLeastOnce",
    guarantee: "at-least-once",
    load,
    remove,
    apply: (pending) =>
      Effect.gen(function* () {
        const store = yield* CheckpointStore;
        const before = beforeRecord(pending);

        const token =
          before.pending === undefined
            ? yield* store.compareAndSet(pending.key, pending.loaded.token, {
                ...before,
                pending: { ranges: detailsOf(pending).ranges },
              })
            : pending.loaded.token;

        yield* pending.run;
        yield* store.compareAndSet(pending.key, token, pending.next);
      }),
  }),
);
