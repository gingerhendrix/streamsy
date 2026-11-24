/* oxlint-disable effecttsgo/any-unknown-in-error-context, anti-slop/no-unknown-parameters -- This internal binding implementation erases the handler and prepared-value types already checked by the constructor overloads. */
/* oxlint-disable typescript/no-explicit-any, anti-slop/no-unsafe-dictionary-type, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- Declarations are the checked bridge between stream declarations and their mapped projection inputs; each assertion is documented at its use. */
import { Array, Option, Record, Schema, type Effect, type Schedule } from "effect";
import { type Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import { resolveOutputs, type RoutedMap, type Services } from "./outputs.ts";
import { validateAccepted, type AcceptedOutput } from "./accepted.ts";
import type { WakeAt } from "./wake-at.ts";
import { supportsPrepare } from "./commit/range.ts";
import * as Commit from "./commit.ts";
import type { Executable } from "./executable.ts";
import type { RunnerFailure } from "./registry.ts";
import type { ProjectionFault } from "./fault.ts";
import type { InputMap, Slices } from "./batch.ts";
import { bindState, type StateDefinition, type BoundState } from "./state.ts";
import { adapt } from "./step.ts";
import { withParams, type Unit } from "./unit.ts";
import {
  GenerationOutputs,
  generationBinding,
  isPerGeneration,
  validateGenerationParam,
  type DeclaredParams,
  type GenerationOutputBindings,
  type PerGeneration,
} from "./per-generation.ts";
import { RoutedOutputs, type RoutedHandle, type RoutedOutputBindings } from "./routed-outputs.ts";

/** A stream declaration. `.at(params)` resolves it with the projection params. */
export type StreamDeclaration = StreamRoute.StreamRoute<any, any> &
  Streamsy.Any & {
    readonly at: (params: any) => Streamsy.Address<any, any>;
  };

/** A stream declaration, or a fixed address that `.at` does not rebind. */
export type Input = StreamDeclaration | Streamsy.Address<any, any>;

export type Inputs = Readonly<Record<string, Input>>;

export type AddressOf<X> =
  X extends PerGeneration<infer D, string, string>
    ? ReturnType<D["at"]>
    : X extends StreamDeclaration
      ? ReturnType<X["at"]>
      : X extends Streamsy.Address<any, any>
        ? X
        : never;

export type Addresses<I extends RoutedMap> = { readonly [K in keyof I]: AddressOf<I[K]> };

type HandlerOutput<X> =
  X extends PerGeneration<infer D, string, infer By>
    ? [By] extends [never]
      ? AddressOf<X>
      : string extends By
        ? AddressOf<X> | RoutedHandle<AddressOf<X>, DecodedParams, D["Identifier"]>
        : RoutedHandle<
            AddressOf<X>,
            Pick<DeclaredParams<D>, By & keyof DeclaredParams<D>>,
            D["Identifier"]
          >
    : AddressOf<X>;

export type HandlerOutputs<O extends RoutedMap> = {
  readonly [K in keyof O]: HandlerOutput<O[K]>;
};

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

type InvalidOutput<C extends StreamRoute.ParamCodecs, X> =
  X extends PerGeneration<infer D, infer N, infer By>
    ? Extract<N | By, keyof C> | Invalid<C, Omit<DeclaredParams<D>, N | By>>
    : InvalidInput<C, X>;

export type OneOutputWithin<C extends StreamRoute.ParamCodecs, X> =
  InvalidOutput<C, X> extends never ? X : never;

export type OutputsWithin<C extends StreamRoute.ParamCodecs, D extends RoutedMap> = {
  readonly [K in keyof D]: OneOutputWithin<C, D[K]>;
};

/** Internal access to the same binding at a chosen generation. */
const BindGeneration = Symbol.for("@streamsy/projection/internal/BindGeneration");

/** Inert: `at` binds the params and returns the value runners take. */
export interface Declaration<
  Codecs extends StreamRoute.ParamCodecs,
  Bound,
  HE = never,
  HR = never,
> {
  readonly retry?: BindingRetry<StreamRoute.Params<Codecs>, HE, HR> | undefined;
  readonly id: string;
  readonly generation: number;
  readonly params: Codecs;
  /** The declared inputs, before `at` binds them. */
  readonly inputs: Inputs;
  /** Throws a RangeError when a param does not encode. */
  readonly at: (params: StreamRoute.Params<Codecs>) => Bound;
  /** Tries declaration inputs in order. Shared inputs return only their declared subset. */
  readonly parse: (id: string) => Option.Option<Partial<StreamRoute.Params<Codecs>>>;
}

export const bindAtGeneration = <C extends StreamRoute.ParamCodecs, Bound>(
  declaration: Pick<Declaration<C, Bound>, "at" | "params"> & {
    readonly [BindGeneration]?: (params: StreamRoute.Params<C>, generation: number) => Bound;
  },
  params: StreamRoute.Params<C>,
  generation: number,
): Bound | undefined => declaration[BindGeneration]?.(params, generation);

export type AcceptedServices<W> = W extends AcceptedOutput ? Services<{ accepted: W }> : never;

export type AcceptedResult<Result, W> = W extends AcceptedOutput
  ? Result extends Commit.Folded<infer A>
    ? Commit.Accumulated<A, {}>
    : Result extends WakeAt
      ? Commit.Emitted<{}> & Pick<WakeAt, "wakeAt">
      : Commit.Emitted<{}>
  : Result;

/** Binding retry timing and budget, shared by both attach hosts. */
export interface BindingRetry<Params, E = never, R = never> {
  /**
   * Timing only: defaults to 1 s doubling to 1 hour. A fresh schedule replays the
   * failure count; input- or elapsed-time-dependent schedules are unsupported.
   * A schedule that ends exhausts the binding.
   */
  readonly schedule?: Schedule.Schedule<unknown>;
  /** Failed runs before stopping. Default: unlimited. */
  readonly maxAttempts?: number;
  /**
   * Runs at least once before exhaustion stops the binding; dedupe with unit.key.
   * Skipped and logged when no unit formed. Hook failures are logged and the binding
   * stops anyway. Non-retryable faults stop without calling the hook.
   */
  readonly onExhausted?: (unit: Unit<Params>, failure: RunnerFailure) => Effect.Effect<void, E, R>;
}

export interface Common<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  W = undefined,
  HE = never,
  HR = never,
> {
  readonly retry?: BindingRetry<StreamRoute.Params<C>, HE, HR>;
  readonly id: string;
  readonly generation?: number;
  readonly params: C;
  readonly inputs: I & Within<C, I>;
  /** Publish accepted input offsets in this wake stream as part of each unit. */
  readonly accepted?: W & OneOutputWithin<C, W>;
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

  return Record.fromEntries(
    Array.map(Record.toEntries(codecs), ([name, codec]) => {
      const encoded = Schema.encodeOption(codec)(values[name]);

      if (Option.isNone(encoded))
        throw new RangeError(`Cannot encode projection ${id} parameter ${name}`);

      return [name, encoded.value];
    }),
  );
};

export const addressesOf = <I extends Inputs>(inputs: I, params: DecodedParams): Addresses<I> => {
  const built: unknown = Record.fromEntries(
    Array.map(Record.toEntries(inputs), ([name, input]) => [
      name,
      isDeclaration(input) ? input.at(params) : input,
    ]),
  );

  // Each entry is resolved under its own key, so the record has the mapped shape.
  return built as Addresses<I>;
};

export const declare = <C extends StreamRoute.ParamCodecs, Bound, HE = never, HR = never>(
  definition: {
    readonly id: string;
    readonly generation?: number;
    readonly params: C;
    readonly inputs: Inputs;
    readonly retry?: BindingRetry<StreamRoute.Params<C>, HE, HR>;
  },
  bind: (key: {
    readonly id: string;
    readonly generation: number;
    readonly params: Record<string, string>;
    readonly decoded: StreamRoute.Params<C>;
  }) => Bound,
): Declaration<C, Bound, HE, HR> => {
  const generation = definition.generation ?? 1;

  const atGeneration = (params: StreamRoute.Params<C>, selected: number) =>
    bind({
      id: definition.id,
      generation: selected,
      params: encodeParams(definition.id, definition.params, params),
      decoded: params,
    });

  const declaration: Declaration<C, Bound, HE, HR> = {
    retry: definition.retry,
    id: definition.id,
    generation,
    params: definition.params,
    inputs: definition.inputs,
    at: (params) => atGeneration(params, generation),
    parse: (id) => {
      for (const input of Record.values<string, Input>(definition.inputs)) {
        if (!isDeclaration(input)) continue;
        const parsed = input.parse(id);

        if (Option.isSome(parsed)) return Option.some(parsed.value);
      }

      return Option.none();
    },
  };

  return Object.defineProperty(declaration, BindGeneration, { value: atGeneration });
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
  HE = never,
  HR = never,
> = Declaration<
  C,
  Omit<
    Executable<Addresses<I>, E | ProjectionFault, R, Q, Result, P, S, OR>,
    "process" | "prepare" | "state"
  > & {
    readonly state: [S] extends [never] ? undefined : BoundState<S>;
    readonly process: (
      batch: Slices<Addresses<I>>,
      unit: Unit,
      encoded?: string,
    ) => Effect.Effect<Result, E | ProjectionFault, R>;
  },
  HE,
  HR
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
  OR = never,
  HE = never,
  HR = never,
> = RunningDeclaration<C, I, E, R, Result, P, Q, A, OR, HE, HR>;

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
  OR = never,
  HE = never,
  HR = never,
> = Omit<RunningDeclaration<C, I, E, R, Result, P, Q, A, Services<O> | OR, HE, HR>, "at"> & {
  readonly at: (params: StreamRoute.Params<C>) => ReturnType<
    RunningDeclaration<C, I, E, R, Result, P, Q, A, Services<O> | OR, HE, HR>["at"]
  > & {
    readonly streams: Readonly<Record<string, Streamsy.Address<unknown, Services<O> | OR>>>;
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
  OR = never,
  HE = never,
  HR = never,
> = OutputDeclaration<C, I, O, E, R, Commit.Accumulated<A, O>, P, Q, A, OR, HE, HR>;

type Kind = "fold" | "process" | "emit" | "mapAccum" | "mapAccumBatch";

/** @internal Reject legacy definitions passed from JavaScript or unchecked TypeScript. */
// oxlint-disable-next-line anti-slop/no-object-parameters -- The guard inspects an extra property on differently typed constructor arguments.
export const rejectProjectionVersion = (id: string, definition: object): void => {
  if (!("version" in definition) || definition.version === undefined) return;

  const advice =
    definition.version === 1
      ? "Remove version: 1 from the declaration."
      : "Raise generation above every generation the projection has used.";

  throw new RangeError(`Projection ${id}: version was removed. ${advice}`);
};

/** Public constructors check all mapped types before this shared binding boundary. */
export interface BuildDefinition extends Omit<
  Common<StreamRoute.ParamCodecs, Inputs, AcceptedOutput | undefined, any, any>,
  "accepted"
> {
  readonly accepted?: AcceptedOutput | undefined;
  readonly commit?: Commit.Commit<any, never, any>;
  readonly state?: StateDefinition<any, any, any>;
  readonly outputs?: RoutedMap;
  readonly fold?: (...args: any[]) => any;
  readonly process?: (...args: any[]) => any;
  readonly emit?: (...args: any[]) => any;
  readonly mapAccum?: (...args: any[]) => any;
  readonly mapAccumBatch?: (...args: any[]) => any;
  readonly prepare?: (...args: any[]) => Effect.Effect<any, any, any>;
}

interface Built extends GenerationOutputBindings, RoutedOutputBindings<Streamsy.Any["Identifier"]> {
  readonly id: string;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Addresses<Inputs>;
  readonly state: BoundState<any> | undefined;
  readonly streams: Addresses<Inputs>;
  readonly accepted: Streamsy.Address<any, any> | undefined;
  readonly commit: Commit.Commit<any, never, any> | typeof Commit.checkpoint;
  readonly prepare:
    | ((batch: Slices<InputMap>, unit: Unit) => Effect.Effect<any, any, any>)
    | undefined;
  readonly process: (
    batch: Slices<InputMap>,
    unit: Unit,
    encoded?: string,
    prepared?: unknown,
    attempt?: number,
  ) => Effect.Effect<any, any, any>;
}

export const build = (
  kind: Kind,
  definition: BuildDefinition,
): Declaration<StreamRoute.ParamCodecs, Built, any, any> => {
  rejectProjectionVersion(definition.id, definition);
  const commit = definition.commit ?? Commit.checkpoint;
  const outputs = definition.outputs ?? {};

  for (const output of Record.values(outputs)) {
    validateGenerationParam(output, definition.params);

    if (isPerGeneration(output) && generationBinding(output).routeBy !== undefined) {
      if (kind !== "process") throw new RangeError("Routed outputs require Projection.process");

      if (!supportsPrepare(commit))
        throw new RangeError("Routed outputs require Commit.transaction");
    }
  }

  validateAccepted(
    definition.accepted,
    definition.params,
    Array.map(
      Array.filter(Record.values(definition.inputs), isDeclaration),
      (input) => input.template,
    ),
    commit,
    Record.keys(outputs),
  );

  if (definition.prepare !== undefined && !supportsPrepare(commit))
    throw new RangeError("prepare requires Commit.transaction");

  const declaration = declare(definition, ({ decoded, ...key }) => {
    const state = definition.state === undefined ? undefined : bindState(definition.state, decoded);
    const inputs = addressesOf(definition.inputs, decoded);

    const { streams, accepted, generationOutputs, routed, handlerOutputs } = resolveOutputs(
      outputs,
      definition.accepted,
      decoded,
      key,
      inputs,
    );

    const bound = {
      ...key,
      inputs,
      state,
      streams,
      accepted,
      commit,
      prepare:
        definition.prepare === undefined
          ? undefined
          : (batch: Slices<InputMap>, unit: Unit) =>
              definition.prepare!(batch, withParams(unit, decoded), { outputs: handlerOutputs }),
      process: (
        batch: Slices<InputMap>,
        unit: Unit,
        encoded?: string,
        prepared?: unknown,
        attempt = 1,
      ) =>
        adapt(
          kind,
          key,
          state,
          streams,
          handlerOutputs,
          definition[kind]!,
          batch,
          withParams(unit, decoded),
          encoded,
          prepared,
          attempt,
        ),
    };

    if (generationOutputs.size > 0)
      Object.assign(bound, { [GenerationOutputs]: generationOutputs });

    if (routed.size > 0) Object.assign(bound, { [RoutedOutputs]: routed });

    return bound;
  });

  return declaration;
};
