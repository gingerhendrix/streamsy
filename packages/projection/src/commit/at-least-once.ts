/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord is a Schema.Struct plain data record. */
import { Effect, Option } from "effect";
import type { WakeAt } from "../wake-at.ts";
import { make } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { beforeRecord, detailsOf, load, remove, pin, withState } from "./internal.ts";

export const atLeastOnce = pin(
  make<void | WakeAt, never, CheckpointStore>({
    name: "atLeastOnce",
    guarantee: "at-least-once",
    load,
    remove,
    apply: (pending) =>
      Effect.gen(function* () {
        const store = yield* CheckpointStore;
        const before = beforeRecord(pending);
        const ranges = detailsOf(pending).ranges;

        const unit = Option.isSome(pending.unit.due)
          ? { ranges, due: pending.unit.due.value }
          : { ranges };

        const token =
          before.pending === undefined
            ? yield* store.compareAndSet(pending.key, pending.loaded.token, {
                ...before,
                pending: unit,
              })
            : pending.loaded.token;

        const result = yield* pending.run;

        yield* store.compareAndSet(pending.key, token, withState(pending.next, result));
      }),
  }),
);
