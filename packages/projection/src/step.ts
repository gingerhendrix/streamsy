/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- The constructor signatures check each adapter before this internal runtime boundary erases its mapped types. */
import { Folded } from "./commit.ts";
import { Effect, Predicate } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";
import { items } from "./read.ts";
import { encodeState, currentState, type BoundState } from "./state.ts";
import type { Unit } from "./unit.ts";

export type Step<X, E = never, R = never> = X | Effect.Effect<X, E, R>;

/** One fold step: the state and one tagged item give the next state. */
export type FoldStep<Inputs extends InputMap, A, E, R, P = Record<string, string>> = (
  state: A,
  entry: Entry<Inputs>,
  unit: Unit<P>,
) => A | Effect.Effect<A, E, R>;

/** Internal: the unit loads the state once, folds in unit order, and returns its encoded state for the checkpoint write. */
export const foldHandler =
  <Inputs extends InputMap, A, E, R, P>(
    key: ProjectionKey,
    state: BoundState<A>,
    step: FoldStep<Inputs, A, E, R, P>,
  ) =>
  (
    batch: Slices<Inputs>,
    unit: Unit<P>,
    encoded?: string,
  ): Effect.Effect<Folded<A>, E | ProjectionFault, R> =>
    Effect.gen(function* () {
      let value = yield* currentState(key, state, encoded);
      for (const entry of items(batch)) {
        const next = step(value, entry, unit);
        value = Effect.isEffect(next) ? yield* next : next;
      }
      return { ...Folded({ value, encoded: yield* encodeState(key, state.schema, value) }), value };
    });

/** Each output keeps its item type here; missing keys emit nothing. */
export type Emits<O extends import("./output-internal.ts").RoutedMap> = Partial<
  import("./output.ts").Items<O>
>;

import { StreamAddress, type Streamsy } from "@streamsy/core";
import type * as Output from "./output.ts";
import { rowsAddress } from "./output-source.ts";
import { Emitted, Accumulated } from "./commit.ts";

export const outputAddresses = (outputs: Output.Map, inputs: InputMap) => {
  const streams: Record<string, Streamsy.Address<unknown, any>> = {};
  for (const [name, output] of Object.entries(outputs)) {
    streams[name] = Predicate.isTagged(output, "Stream")
      ? Object.assign(StreamAddress.json(output.stream.id, { schema: output.schema }), {
          stream: output.stream.stream,
        })
      : rowsAddress(name, output);
  }
  const ids = Object.values(streams).map((address) => address.id);
  if (new Set(ids).size !== ids.length)
    throw new RangeError("Two outputs may not name the same stream");
  const inputIds = new Set(Object.values(inputs).map((address) => address.id));
  if (ids.some((id) => inputIds.has(id)))
    throw new RangeError("An output may not name an input stream");
  return streams;
};

const collect = (
  outputs: Output.Map,
  buffers: Record<string, unknown[]>,
  emitted: Readonly<Record<string, ReadonlyArray<any>>>,
) =>
  Effect.gen(function* () {
    for (const [name, entries] of Object.entries(emitted)) {
      const output = outputs[name];
      if (output === undefined || !Array.isArray(entries))
        return yield* new ProjectionFault({
          phase: "process",
          reason: "invalid-output",
          message: `Invalid output array ${name}`,
        });
      const values = Predicate.isTagged(output, "Stream")
        ? entries
        : entries.map((change: Output.Change<any>) =>
            Predicate.isTagged(change, "Upsert")
              ? {
                  type: name,
                  key: String(change.row[output.key]),
                  value: change.row,
                  headers: { operation: "upsert" },
                }
              : { type: name, key: String(change.key), headers: { operation: "delete" } },
          );
      (buffers[name] ??= []).push(...values);
    }
  });

/** One adapter invocation is one unit. Per-entry variants thread state and preserve order. */
export const adapt = (
  kind: "fold" | "process" | "emit" | "mapAccum" | "mapAccumBatch",
  key: ProjectionKey,
  state: BoundState<any> | undefined,
  outputs: Output.Map,
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
    const buffers: Record<string, unknown[]> = {};
    const steps = kind === "mapAccumBatch" ? [batch] : items(batch);
    for (const entry of steps) {
      const step = kind === "emit" ? handler(entry, unit) : handler(value, entry, unit);
      const result = Effect.isEffect(step) ? yield* step : step;
      if (kind === "fold") value = result;
      else {
        const emitted = kind === "emit" ? result : result[1];
        if (kind !== "emit") value = result[0];
        yield* collect(outputs, buffers, emitted);
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
