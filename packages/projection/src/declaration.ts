/* oxlint-disable effecttsgo/any-unknown-in-error-context, anti-slop/no-unknown-parameters -- This internal binding implementation erases the handler and prepared-value types already checked by the constructor overloads. */
/* oxlint-disable typescript/no-explicit-any, anti-slop/no-unsafe-dictionary-type, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- Declarations are the checked bridge between stream declarations and their mapped projection inputs; each assertion is documented at its use. */
import { Option, Schema, type Effect } from "effect";
import { type Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import { resolveOutputs, type RoutedMap, type Services } from "./outputs.ts";
import { supportsPrepare } from "./commit/range.ts";
import * as Commit from "./commit.ts";
import type { Executable } from "./executable.ts";
import type { ProjectionFault } from "./fault.ts";
import type { InputMap, Slices } from "./batch.ts";
import { bindState, type StateDefinition } from "./state.ts";
import { adapt } from "./step.ts";
import { withParams, type Unit } from "./unit.ts";

/** A stream declaration. `.at(params)` resolves it with the projection params. */
export type StreamDeclaration = StreamRoute.StreamRoute<any, any> &
  Streamsy.Any & {
    readonly at: (params: any) => Streamsy.Address<any, any>;
  };

/** A stream declaration, or a fixed address that `.at` does not rebind. */
export type Input = StreamDeclaration | Streamsy.Address<any, any>;

export type Inputs = Readonly<Record<string, Input>>;

export type AddressOf<X> = X extends StreamDeclaration
  ? ReturnType<X["at"]>
  : X extends Streamsy.Address<any, any>
    ? X
    : never;

export type Addresses<I extends Inputs> = { readonly [K in keyof I]: AddressOf<I[K]> };

type DeclaredParams<X> = X extends StreamRoute.StreamRoute<infer P, any, any, any> ? P : {};

type Invalid<C extends StreamRoute.ParamCodecs, P> = {
  readonly [Name in keyof P]: Name extends keyof StreamRoute.Params<C>
    ? P[Name] extends StreamRoute.Params<C>[Name]
      ? StreamRoute.Params<C>[Name] extends P[Name]
        ? never
        : Name
      : Name
    : Name;
}[keyof P];

type InvalidInput<C extends StreamRoute.ParamCodecs, X> = X extends StreamDeclaration
  ? Invalid<C, DeclaredParams<X>>
  : never;

/** Every declaration's params must be projection params with the same type. */
export type Within<C extends StreamRoute.ParamCodecs, I extends Inputs> = {
  readonly [K in keyof I]: InvalidInput<C, I[K]> extends never ? I[K] : never;
};

export type OneWithin<C extends StreamRoute.ParamCodecs, X> =
  InvalidInput<C, X> extends never ? X : never;

export type OutputsWithin<C extends StreamRoute.ParamCodecs, D extends RoutedMap> = Within<C, D>;

/** Inert: `at` binds the params and returns the value runners take. */
export interface Declaration<Codecs extends StreamRoute.ParamCodecs, Bound> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Codecs;
  /** Throws a RangeError when a param does not encode. */
  readonly at: (params: StreamRoute.Params<Codecs>) => Bound;
  /** Tries declaration inputs in order. Shared inputs return only their declared subset. */
  readonly parse: (id: string) => Option.Option<Partial<StreamRoute.Params<Codecs>>>;
}

export interface Common<C extends StreamRoute.ParamCodecs, I extends Inputs> {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: C;
  readonly inputs: I & Within<C, I>;
}

/** Decoded params, keyed by name. */
export type DecodedParams = Readonly<Record<string, unknown>>;

export const isDeclaration = (input: Input): input is StreamDeclaration => "at" in input;

const encodeParams = <C extends StreamRoute.ParamCodecs>(
  id: string,
  codecs: C,
  params: StreamRoute.Params<C>,
): Record<string, string> => {
  const values: Readonly<Record<string, unknown>> = params;

  return Object.fromEntries(
    Object.entries(codecs).map(([name, codec]) => {
      const encoded = Schema.encodeOption(codec)(values[name]);

      if (Option.isNone(encoded))
        throw new RangeError(`Cannot encode projection ${id} parameter ${name}`);

      return [name, encoded.value];
    }),
  );
};

export const addressesOf = <I extends Inputs>(inputs: I, params: DecodedParams): Addresses<I> => {
  const built: unknown = Object.fromEntries(
    Object.entries(inputs).map(([name, input]) => [
      name,
      isDeclaration(input) ? input.at(params) : input,
    ]),
  );

  // Each entry is resolved under its own key, so the record has the mapped shape.
  return built as Addresses<I>;
};

export const declare = <C extends StreamRoute.ParamCodecs, Bound>(
  definition: {
    readonly id: string;
    readonly version?: number;
    readonly generation?: number;
    readonly params: C;
    readonly inputs: Inputs;
  },
  bind: (key: {
    readonly id: string;
    readonly version: number;
    readonly generation: number;
    readonly params: Record<string, string>;
    readonly decoded: StreamRoute.Params<C>;
  }) => Bound,
): Declaration<C, Bound> => {
  const version = definition.version ?? 1;
  const generation = definition.generation ?? 1;

  return {
    id: definition.id,
    version,
    generation,
    params: definition.params,
    at: (params) =>
      bind({
        id: definition.id,
        version,
        generation,
        params: encodeParams(definition.id, definition.params, params),
        decoded: params,
      }),
    parse: (id) => {
      for (const input of Object.values<Input>(definition.inputs)) {
        if (!isDeclaration(input)) continue;
        const parsed = input.parse(id);

        if (Option.isSome(parsed)) return Option.some(parsed.value);
      }

      return Option.none();
    },
  };
};

export interface Committing<Result, P, Q> {
  readonly commit: Commit.Commit<Result, P, Q>;
}

export type RunningDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  E,
  R,
  Result,
  P,
  Q,
  S = never,
  OR = never,
> = Declaration<
  C,
  Omit<
    Executable<Addresses<I>, E | ProjectionFault, R, Q, Result, P, S, OR>,
    "process" | "prepare"
  > & {
    readonly process: (
      batch: Slices<Addresses<I>>,
      unit: Unit,
      encoded?: string,
    ) => Effect.Effect<Result, E | ProjectionFault, R>;
  }
>;

export type StatefulDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  A,
  E,
  R,
  Result,
  P,
  Q,
> = RunningDeclaration<C, I, E, R, Result, P, Q, A>;

export type OutputDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  O extends RoutedMap,
  E,
  R,
  Result,
  P,
  Q,
  A = never,
> = Omit<RunningDeclaration<C, I, E, R, Result, P, Q, A, Services<O>>, "at"> & {
  readonly at: (params: StreamRoute.Params<C>) => ReturnType<
    RunningDeclaration<C, I, E, R, Result, P, Q, A, Services<O>>["at"]
  > & {
    readonly streams: Readonly<Record<string, Streamsy.Address<unknown, Services<O>>>>;
  };
};

export type AccumulatingDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  O extends RoutedMap,
  A,
  E,
  R,
  P,
  Q,
> = OutputDeclaration<C, I, O, E, R, Commit.Accumulated<A, O>, P, Q, A>;

type Kind = "fold" | "process" | "emit" | "mapAccum" | "mapAccumBatch";

/** Public constructors check all mapped types before this shared binding boundary. */
export interface BuildDefinition extends Common<StreamRoute.ParamCodecs, Inputs> {
  readonly commit?: Commit.Commit<any, any, any>;
  readonly state?: StateDefinition<any, any, any>;
  readonly outputs?: RoutedMap;
  readonly fold?: (...args: any[]) => any;
  readonly process?: (...args: any[]) => any;
  readonly emit?: (...args: any[]) => any;
  readonly mapAccum?: (...args: any[]) => any;
  readonly mapAccumBatch?: (...args: any[]) => any;
  readonly prepare?: (...args: any[]) => Effect.Effect<any, any, any>;
}

export const build = (kind: Kind, definition: BuildDefinition) => {
  const commit = definition.commit ?? Commit.checkpoint;

  if (definition.prepare !== undefined && !supportsPrepare(commit))
    throw new RangeError("prepare requires Commit.transaction");

  const declaration = declare(definition, ({ decoded, ...key }) => {
    const state = definition.state === undefined ? undefined : bindState(definition.state, decoded);
    const inputs = addressesOf(definition.inputs, decoded);
    const streams = resolveOutputs(definition.outputs ?? {}, decoded, inputs);

    return {
      ...key,
      inputs,
      state,
      streams,
      commit,
      prepare:
        definition.prepare === undefined
          ? undefined
          : (batch: Slices<InputMap>, unit: Unit) =>
              definition.prepare!(batch, withParams(unit, decoded)),
      process: (batch: Slices<InputMap>, unit: Unit, encoded?: string, prepared?: unknown) =>
        adapt(
          kind,
          key,
          state,
          streams,
          definition[kind]!,
          batch,
          withParams(unit, decoded),
          encoded,
          prepared,
        ),
    };
  });

  return declaration;
};
