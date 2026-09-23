/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This constructor erases declaration-specific types only after the mapped public signature checks them. */
import { Effect, Option, Schema } from "effect";
import { StreamRef } from "@streamsy/core";
import type { InputMap, Slices } from "./batch.ts";
import type * as Output from "./output.ts";
import { ProjectionFault } from "./fault.ts";
import { loadState, type Initial, type OutputFold } from "./fold.ts";
import { bind, rowsRef, type Bound } from "./output-source.ts";
import { State } from "./state.ts";
import type { Unit } from "./unit.ts";

/** Normalized stream-form declaration consumed by the pinned kernel. */
export interface Named<Inputs extends InputMap, E, R> {
  readonly _tag: "Outputs";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly streams: Readonly<Record<string, StreamRef.StreamRef<unknown>>>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<Output.Processed, E, R>;
}
const initialValue = <A>(initial: Initial<A>, params: Record<string, string>): A =>
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Initial explicitly accepts a params factory.
  typeof initial === "function"
    ? (initial as (params: Record<string, string>) => A)(params)
    : initial;

export function outputs<
  Inputs extends InputMap,
  const D extends Output.Map,
  Result extends Output.Result<D>,
  E,
  R,
>(definition: {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params?: Record<string, string>;
  readonly inputs: Inputs;
  readonly outputs: D;
  readonly process: (
    | ((batch: Slices<Inputs>, unit: Unit) => Effect.Effect<Result, E, R>)
    | OutputFold<Inputs, Output.StateOf<D>, Result, E, R>
  ) &
    (Exclude<keyof Result, keyof Output.Result<D>> extends never
      ? unknown
      : { readonly invalidOutputKeys: never });
}): Named<Inputs, E | ProjectionFault, R | State> & { readonly outputs: Bound<D, {}> } {
  const streams: Record<string, StreamRef.StreamRef<unknown>> = {};
  const values = Object.values(definition.outputs).filter((output) => output._tag === "Value");
  if (values.length > 1) throw new RangeError("A projection may declare at most one value output");
  if ("state" in definition.outputs)
    throw new RangeError("The output name state is reserved for fold state");
  const value = values[0];
  for (const [name, output] of Object.entries(definition.outputs)) {
    if (output._tag === "Value") continue;
    streams[name] = output._tag === "Stream" ? output.stream : rowsRef(name, output);
  }
  const ids = Object.values(streams).map((ref) => ref.id);
  if (new Set(ids).size !== ids.length)
    throw new RangeError("Two outputs may not name the same stream");
  const inputIds = new Set(Object.values(definition.inputs).map((ref) => ref.id));
  if (ids.some((id) => inputIds.has(id)))
    throw new RangeError("An output may not name an input stream");
  const handler = definition.process;
  if ("_tag" in handler && value === undefined)
    throw new RangeError("An output fold requires an Output.value declaration");
  const key = {
    id: definition.id,
    version: definition.version ?? 1,
    generation: definition.generation ?? 1,
    params: definition.params ?? {},
  };
  return {
    _tag: "Outputs",
    ...key,
    inputs: definition.inputs,
    outputs: bind(definition.outputs, definition.id, Schema.Struct({}), () => ({
      ...key,
      outputs: definition.outputs,
    })),
    streams,
    process: (batch, unit) =>
      Effect.gen(function* () {
        let result: Result;
        if ("_tag" in handler) {
          const previous = yield* loadState(key, value!.schema);
          const next = handler.step(
            Option.isSome(previous) ? previous.value : initialValue(handler.initial, key.params),
            batch,
            unit,
          );
          result = Effect.isEffect(next) ? yield* next : next;
        } else result = yield* handler(batch, unit);
        const items: Record<string, ReadonlyArray<unknown>> = {};
        for (const [name, output] of Object.entries(definition.outputs)) {
          if (output._tag === "Value") continue;
          const entries = (result as Record<string, ReadonlyArray<any>>)[name];
          if (!Array.isArray(entries))
            return yield* new ProjectionFault({
              phase: "process",
              reason: "invalid-output",
              message: `Missing output array ${name}`,
            });
          items[name] =
            output._tag === "Stream"
              ? entries
              : entries.map((change: Output.Change<any>) =>
                  change._tag === "Upsert"
                    ? {
                        type: name,
                        key: String(change.row[output.key]),
                        value: change.row,
                        headers: { operation: "upsert" },
                      }
                    : { type: name, key: String(change.key), headers: { operation: "delete" } },
                );
        }
        if (value === undefined) return { items };
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(value.schema))(
          (result as { state: unknown }).state,
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ProjectionFault({
                phase: "process",
                reason: "invalid-output",
                message: `Cannot encode state for ${key.id}`,
                cause,
              }),
          ),
        );
        const state = yield* State;
        return { items, saveState: state.save(key, encoded) };
      }),
  };
}
