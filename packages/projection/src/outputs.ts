/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This constructor erases declaration-specific types only after the mapped public signature checks them. */
import { Effect, Schema } from "effect";
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
import { bind, rowsAddress, stateSource, type Bound, type StateSource } from "./output-source.ts";
import {
  State,
  bindState,
  encodeState,
  currentState,
  type BoundState,
  type StateDefinition,
} from "./state.ts";
import { withParams, type Unit } from "./unit.ts";

/** Normalized outputs declaration consumed by the pinned kernel. */
export interface Outputs<Inputs extends InputMap, E, R, OR = never, S = unknown> {
  readonly _tag: "Outputs";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  /** Set when the declaration has `state:`. */
  readonly state?: BoundState<S>;
  readonly streams: Readonly<Record<string, Streamsy.Address<unknown, OR>>>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<Output.Processed, E, R>;
}

type OnlyDeclared<Result, Allowed> =
  Exclude<keyof Result, Allowed> extends never ? unknown : { readonly invalidOutputKeys: never };

/**
 * Without `state:`, the handler takes the batch and the unit. With `state:`, it gets
 * the prior state first and returns the next state as `state`, next to the output arrays.
 */
export interface OutputsDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  D extends RoutedMap,
  Result extends Output.Result<D, A>,
  E,
  R,
  A = never,
  SI = never,
> extends Common<C, I> {
  readonly outputs: D & OutputsWithin<C, D>;
  readonly state?: StateDefinition<A, SI, StreamRoute.Params<C>>;
  readonly process: ([A] extends [never]
    ? (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Result, E, R>
    : (
        state: NoInfer<A>,
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Result, E, R>) &
    OnlyDeclared<Result, keyof Output.Result<D, A>>;
}

/** Checks that do not depend on params run when the projection is declared. */
const checkDeclarations = (outputs: RoutedMap) => {
  if ("state" in outputs) throw new RangeError("The output name state is reserved for the state");
};

/** A bound outputs declaration. With `state:`, it also gives `state`, a source for `Serve.value`. */
export type OutputsDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  D extends RoutedMap,
  A,
  SI,
  E,
  R,
> = Declaration<C, Outputs<Addresses<I>, E | ProjectionFault, R | State, Output.Services<D>, A>> & {
  readonly outputs: Bound<D, StreamRoute.Params<C>>;
} & ([A] extends [never] ? {} : { readonly state: StateSource<A, SI, StreamRoute.Params<C>> });

export function outputs<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const D extends RoutedMap,
  Result extends Output.Result<D, A>,
  E,
  R,
  A = never,
  SI = never,
>(
  definition: OutputsDefinition<C, I, D, Result, E, R, A, SI>,
): OutputsDeclaration<C, I, D, A, SI, E, R>;
export function outputs(
  definition: Common<StreamRoute.ParamCodecs, Inputs> & {
    readonly outputs: RoutedMap;
    readonly state?: StateDefinition<unknown, unknown, any>;
    readonly process: (...args: any[]) => Effect.Effect<any, any, any>;
  },
): Declaration<StreamRoute.ParamCodecs, Outputs<InputMap, unknown, unknown, unknown>> & {
  readonly outputs: unknown;
  readonly state?: unknown;
} {
  checkDeclarations(definition.outputs);
  const state = definition.state;
  const declaration = declare(definition, ({ decoded, ...key }) =>
    bindOutputs(
      {
        ...key,
        inputs: addressesOf(definition.inputs, decoded),
        outputs: resolveOutputs(definition.outputs, decoded),
        state: state === undefined ? undefined : bindState(state, decoded),
        process: definition.process,
      },
      decoded,
    ),
  );
  const keyAt = (params: StreamRoute.Params<StreamRoute.ParamCodecs>) => {
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
  const schema = Schema.Struct(definition.params) as StreamRoute.PathSchema<
    StreamRoute.Params<StreamRoute.ParamCodecs>
  >;
  const outputs = bind(definition.outputs, definition.id, schema, keyAt);
  return state === undefined
    ? { ...declaration, outputs }
    : { ...declaration, outputs, state: stateSource(definition, state, declaration.at) };
}

interface BoundDefinition<Inputs extends InputMap, Result, E, R> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly outputs: Output.Map;
  readonly state?: BoundState<unknown>;
  /** Takes the prior state first when `state` is present. */
  readonly process: (...args: any[]) => Effect.Effect<Result, E, R>;
}

/** Internal: the value `.at(params)` returns. Params are already encoded and decoded. */
const bindOutputs = <Inputs extends InputMap, Result, E, R>(
  definition: BoundDefinition<Inputs, Result, E, R>,
  decoded: DecodedParams,
): Outputs<Inputs, E | ProjectionFault, R | State, any> => {
  const streams: Record<string, Streamsy.Address<unknown, unknown>> = {};
  const state = definition.state;
  for (const [name, output] of Object.entries(definition.outputs)) {
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
    state,
    streams,
    process: (batch, unit) =>
      Effect.gen(function* () {
        const view = withParams(unit, decoded);
        const result =
          state === undefined
            ? yield* handler(batch, view)
            : yield* handler(yield* currentState(key, state), batch, view);
        const items: Record<string, ReadonlyArray<unknown>> = {};
        for (const [name, output] of Object.entries(definition.outputs)) {
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
        if (state === undefined) return { items };
        const encoded = yield* encodeState(
          key,
          state.schema,
          (result as { state: unknown }).state,
          "process",
        );
        const store = yield* State;
        return { items, saveState: store.save(key, encoded) };
      }),
  };
  return Object.defineProperty(result, StateOwner, {
    value: state === undefined ? Effect.succeed(undefined) : Effect.map(State, ownerOf),
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
