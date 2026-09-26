/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This constructor erases declaration-specific types only after the mapped public signature checks them. */
import { Effect, Option, Schema } from "effect";
import { ownerOf } from "@streamsy/core/internal/memory";
import { StreamAddress, type Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { InputMap, Slices } from "./batch.ts";
import {
  addressesOf,
  declare,
  resolveOutputs,
  type Addresses,
  type Common,
  type Declaration,
  type DecodedParams,
  type Inputs,
  type OutputsWithin,
} from "./declaration.ts";
import type * as Output from "./output.ts";
import type { RoutedMap } from "./output-internal.ts";
import { ProjectionFault } from "./fault.ts";
import { loadState, type Initial, type OutputFold } from "./fold.ts";
import { bind, rowsAddress, type Bound } from "./output-source.ts";
import { State } from "./state.ts";
import { withParams, type Unit } from "./unit.ts";

/** Normalized outputs declaration consumed by the pinned kernel. */
export interface Outputs<Inputs extends InputMap, E, R, OR = never> {
  readonly _tag: "Outputs";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly streams: Readonly<Record<string, Streamsy.Address<unknown, OR>>>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<Output.Processed, E, R>;
}
const initialValue = <A, P>(initial: Initial<A, P>, params: P): A =>
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Initial explicitly accepts a params factory.
  typeof initial === "function" ? (initial as (params: P) => A)(params) : initial;

export interface OutputsDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  D extends RoutedMap,
  Result extends Output.Result<D>,
  E,
  R,
> extends Common<C, I> {
  readonly outputs: D & OutputsWithin<C, D>;
  readonly process: (
    | ((
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Result, E, R>)
    | OutputFold<Addresses<I>, Output.StateOf<D>, Result, E, R, StreamRoute.Params<C>>
  ) &
    (Exclude<keyof Result, keyof Output.Result<D>> extends never
      ? unknown
      : { readonly invalidOutputKeys: never });
}

/** Checks that do not depend on params run when the projection is declared. */
const checkDeclarations = (outputs: RoutedMap, fold: boolean) => {
  const values = Object.values(outputs).filter((output) => output._tag === "Value");
  if (values.length > 1) throw new RangeError("A projection may declare at most one value output");
  if ("state" in outputs) throw new RangeError("The output name state is reserved for fold state");
  if (fold && values.length === 0)
    throw new RangeError("An output fold requires an Output.value declaration");
};

export function outputs<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const D extends RoutedMap,
  Result extends Output.Result<D>,
  E,
  R,
>(
  definition: OutputsDefinition<C, I, D, Result, E, R>,
): Declaration<C, Outputs<Addresses<I>, E | ProjectionFault, R | State, Output.Services<D>>> & {
  readonly outputs: Bound<D, StreamRoute.Params<C>>;
} {
  checkDeclarations(definition.outputs, "_tag" in definition.process);
  const declaration = declare(definition, ({ decoded, ...key }) =>
    bindOutputs(
      {
        ...key,
        inputs: addressesOf(definition.inputs, decoded),
        outputs: resolveOutputs(definition.outputs, decoded),
        process: definition.process as BoundDefinition<InputMap, any, E, R>["process"],
      },
      decoded,
    ),
  );
  const keyAt = (params: StreamRoute.Params<C>) => {
    const bound = declaration.at(params);
    return {
      id: bound.id,
      version: bound.version,
      generation: bound.generation,
      params: bound.params,
      outputs: resolveOutputs(definition.outputs, params),
    };
  };
  // Struct owns exactly the codec keys and their decoded parameter types.
  const schema = Schema.Struct(definition.params) as StreamRoute.PathSchema<StreamRoute.Params<C>>;
  return {
    ...declaration,
    outputs: bind(definition.outputs, definition.id, schema, keyAt),
  } as Declaration<C, Outputs<Addresses<I>, E | ProjectionFault, R | State, Output.Services<D>>> & {
    readonly outputs: Bound<D, StreamRoute.Params<C>>;
  };
}

interface BoundDefinition<Inputs extends InputMap, Result, E, R> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly outputs: Output.Map;
  readonly process:
    | ((batch: Slices<Inputs>, unit: Unit<any>) => Effect.Effect<Result, E, R>)
    | OutputFold<Inputs, unknown, Result, E, R, any>;
}

/** Internal: the value `.at(params)` returns. Params are already encoded and decoded. */
const bindOutputs = <Inputs extends InputMap, Result, E, R>(
  definition: BoundDefinition<Inputs, Result, E, R>,
  decoded: DecodedParams,
): Outputs<Inputs, E | ProjectionFault, R | State, any> => {
  const streams: Record<string, Streamsy.Address<unknown, unknown>> = {};
  const value = Object.values(definition.outputs).find((output) => output._tag === "Value");
  for (const [name, output] of Object.entries(definition.outputs)) {
    if (output._tag === "Value") continue;
    streams[name] =
      output._tag === "Stream"
        ? Object.assign(StreamAddress.json(output.stream.id, { schema: output.schema }), {
            stream: output.stream.stream,
          })
        : rowsAddress(name, output);
  }
  const ids = Object.values(streams).map((address) => address.id);
  if (new Set(ids).size !== ids.length)
    throw new RangeError("Two outputs may not name the same stream");
  const inputIds = new Set(Object.values(definition.inputs).map((address) => address.id));
  if (ids.some((id) => inputIds.has(id)))
    throw new RangeError("An output may not name an input stream");
  const handler = definition.process;
  const key = {
    id: definition.id,
    version: definition.version,
    generation: definition.generation,
    params: definition.params,
  };
  const result: Outputs<Inputs, E | ProjectionFault, R | State, any> = {
    _tag: "Outputs",
    ...key,
    inputs: definition.inputs,
    streams,
    process: (batch, unit) =>
      Effect.gen(function* () {
        let result: Result;
        if ("_tag" in handler) {
          const previous = yield* loadState(key, value!.schema);
          const next = handler.step(
            Option.isSome(previous) ? previous.value : initialValue(handler.initial, decoded),
            batch,
            withParams(unit, decoded),
          );
          result = Effect.isEffect(next) ? yield* next : next;
        } else result = yield* handler(batch, withParams(unit, decoded));
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
  return Object.defineProperty(result, StateOwner, {
    value: value === undefined ? Effect.succeed(undefined) : Effect.map(State, ownerOf),
    // Declaration copies must retain the dynamic State lookup; this is not a wire ownership claim.
    enumerable: true,
  });
};

const StateOwner = Symbol.for("@streamsy/projection/internal/StateOwner");
/** Private normalized metadata; the public Outputs declaration does not gain a required field. */
export const stateOwner = <Inputs extends InputMap, E, R, OR>(
  projection: Outputs<Inputs, E, R, OR>,
): Effect.Effect<object | undefined, never, R> => {
  if (!(StateOwner in projection)) return Effect.succeed(undefined);
  // SAFETY: outputs attaches an owner lookup whose State requirement is already included in R.
  return projection[StateOwner] as Effect.Effect<object | undefined, never, R>;
};
