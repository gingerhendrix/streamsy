/* oxlint-disable effecttsgo/any-unknown-in-error-context, anti-slop/no-unknown-parameters -- The public constructor overloads retain exact E, R and prepared types; this adapter is their internal erased implementation. */
/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- The constructor signatures check each adapter before this internal runtime boundary erases its mapped types. */
import { Folded } from "./commit.ts";
import { Effect } from "effect";
import type { InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint-store.ts";
import { items } from "./read.ts";
import { encodeState, currentState, type BoundState } from "./state.ts";
import type { Unit } from "./unit.ts";

export type Step<X, E = never, R = never> = X | Effect.Effect<X, E, R>;

/** Each output keeps its item type here; missing keys emit nothing. */
export type Emits<O extends import("./outputs.ts").RoutedMap> = {
  readonly [K in keyof O]?: ReadonlyArray<import("./outputs.ts").Item<O[K]>>;
};

import type { Streamsy } from "@streamsy/core";
import { collect } from "./outputs.ts";
import { Emitted, Accumulated } from "./commit.ts";

/** One adapter invocation is one unit. Per-entry variants thread state and preserve order. */
export const adapt = (
  kind: "fold" | "process" | "emit" | "mapAccum" | "mapAccumBatch",
  key: ProjectionKey,
  state: BoundState<any> | undefined,
  streams: Record<string, Streamsy.Address<unknown, any>>,
  handler: (...args: any[]) => any,
  batch: Slices<InputMap>,
  unit: Unit<any>,
  encoded?: string,
  prepared?: unknown,
): Effect.Effect<any, any, any> =>
  Effect.gen(function* () {
    if (kind === "process") return yield* Effect.asVoid(handler(batch, unit, prepared));
    let value = state === undefined ? undefined : yield* currentState(key, state, encoded);

    const buffers: Record<string, unknown[]> = Object.fromEntries(
      Object.keys(streams).map((name) => [name, []]),
    );

    const steps = kind === "mapAccumBatch" ? [batch] : items(batch);

    for (const entry of steps) {
      const step = kind === "emit" ? handler(entry, unit) : handler(value, entry, unit);
      const result = Effect.isEffect(step) ? yield* step : step;

      if (kind === "fold") value = result;
      else {
        const emitted = kind === "emit" ? result : result[1];

        if (kind !== "emit") value = result[0];
        yield* collect(streams, buffers, emitted);
      }
    }

    const batchOutputs = Object.fromEntries(
      Object.entries(streams).map(([name, address]) => [
        name,
        { address, items: buffers[name] ?? [] },
      ]),
    );

    if (state === undefined) return Emitted({ outputs: batchOutputs });
    const next = yield* encodeState(key, state.schema, value);

    return kind === "fold"
      ? Folded({ value, encoded: next })
      : Accumulated({ value, encoded: next, outputs: batchOutputs });
  });
