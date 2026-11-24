/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { Effect } from "effect";
import type { AcceptedOutput } from "./accepted.ts";
import type { Commit, TransactionCommit } from "./commit.ts";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Slices } from "./batch.ts";
import {
  build,
  type AcceptedServices,
  type AcceptedResult,
  type Common,
  type Inputs,
  type Addresses,
  type HandlerOutputs,
  type RunningDeclaration,
  type Committing,
  type OutputDeclaration,
  type OutputsWithin,
} from "./declaration.ts";
import type { RoutedMap } from "./outputs.ts";
import type { WakeAt } from "./wake-at.ts";
import type { Unit } from "./unit.ts";

/** void unless the handler result can be a WakeAt. */
type TimerResult<A> = 0 extends 1 & A
  ? void
  : [Extract<A, WakeAt>] extends [never]
    ? void
    : void | WakeAt;

/** The third argument of `process`. */
export interface ProcessContext<Prepared = void> {
  readonly prepared: Prepared;
  /** Binding run attempt, starting at 1. Always 1 under run and follow. */
  readonly attempt: number;
}

/** The address or routed handle of each declared output. Open handles only inside `process`. */
export interface OutputsContext<O extends RoutedMap> {
  readonly outputs: HandlerOutputs<O>;
}

type TransactionBrand = Omit<TransactionCommit<never, never>, keyof Commit<never>>;

type WithTransactionCommit<D extends { readonly at: (...args: never[]) => object }> = Omit<
  D,
  "at"
> & {
  readonly at: (...args: Parameters<D["at"]>) => ReturnType<D["at"]> & {
    readonly commit: TransactionBrand;
  };
};

/** Run side effects once per unit, including recorded wakes. Return wakeAt(time) to
 * replace the timer, or void to clear it. Timers require atLeastOnce or transaction.
 * prepare runs at least once, outside the transaction. */
export function process<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  E = never,
  R = never,
  P = never,
  Q = never,
  Prepared = void,
  PE = never,
  PR = never,
  W extends AcceptedOutput | undefined = undefined,
  HE = never,
  HR = never,
  A = void,
>(
  definition: Common<C, I, W, HE, HR> &
    Committing<AcceptedResult<TimerResult<A>, W>, P, Q> & {
      readonly outputs?: never;
      readonly prepare?: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Prepared, PE, PR>;
      readonly process: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
        context: ProcessContext<Prepared>,
      ) => Effect.Effect<A, E, R>;
    },
): RunningDeclaration<
  C,
  I,
  E | PE,
  R,
  TimerResult<A>,
  P,
  Q | PR,
  never,
  AcceptedServices<W>,
  HE,
  HR
>;
export function process<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const O extends RoutedMap,
  E = never,
  R = never,
  Q = never,
  Prepared = void,
  PE = never,
  PR = never,
  W extends AcceptedOutput | undefined = undefined,
  HE = never,
  HR = never,
  A = void,
>(
  definition: Common<C, I, W, HE, HR> &
    Committing<AcceptedResult<TimerResult<A>, W>, never, Q> & {
      readonly commit: TransactionBrand;
      readonly outputs: O & OutputsWithin<C, O>;
      readonly prepare?: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
        context: OutputsContext<O>,
      ) => Effect.Effect<Prepared, PE, PR>;
      readonly process: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
        context: ProcessContext<Prepared> & OutputsContext<O>,
      ) => Effect.Effect<A, E, R>;
    },
): WithTransactionCommit<
  OutputDeclaration<
    C,
    I,
    O,
    E | PE,
    R,
    TimerResult<A>,
    never,
    Q | PR,
    never,
    AcceptedServices<W>,
    HE,
    HR
  >
>;
export function process(definition: Parameters<typeof build>[1]): unknown {
  return build("process", definition);
}
