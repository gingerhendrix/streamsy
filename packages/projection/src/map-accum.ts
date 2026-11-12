/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { WakeOutput } from "./wake.ts";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry, Slices } from "./batch.ts";
import type { Accumulated } from "./commit.ts";
import {
  build,
  type AcceptedServices,
  type Common,
  type Inputs,
  type Addresses,
  type AccumulatingDeclaration,
  type Committing,
  type OutputsWithin,
} from "./declaration.ts";
import type { RoutedMap } from "./outputs.ts";
import type { StateDefinition } from "./state.ts";
import type { Step, Emits } from "./step.ts";
import type { Unit } from "./unit.ts";

type Definition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  O extends RoutedMap,
  A,
  SI,
  P,
  Q,
  W,
> = Common<C, I, W> &
  Committing<Accumulated<NoInfer<A>, NoInfer<O>>, P, Q> & {
    readonly outputs: O & OutputsWithin<C, O>;
    readonly state: StateDefinition<A, SI, StreamRoute.Params<C>>;
  };

/** Thread the value and collect outputs, once per entry. */
export function mapAccum<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const O extends RoutedMap,
  A,
  SI,
  X extends Emits<O>,
  E = never,
  R = never,
  P = never,
  Q = never,
  W extends WakeOutput | undefined = undefined,
>(
  definition: Definition<C, I, O, A, SI, P, Q, W> & {
    readonly mapAccum: ((
      state: NoInfer<A>,
      entry: Entry<Addresses<I>>,
      unit: Unit<StreamRoute.Params<C>>,
    ) => Step<readonly [A, X], E, R>) &
      (Exclude<keyof X, keyof O> extends never
        ? unknown
        : { readonly undeclaredOutput: Exclude<keyof X, keyof O> });
  },
): AccumulatingDeclaration<C, I, O, A, E, R, P, Q, AcceptedServices<W>>;
export function mapAccum(definition: Parameters<typeof build>[1]): unknown {
  return build("mapAccum", definition);
}

/** Compute the next value and outputs once for the whole unit batch. */
export function mapAccumBatch<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const O extends RoutedMap,
  A,
  SI,
  X extends Emits<O>,
  E = never,
  R = never,
  P = never,
  Q = never,
  W extends WakeOutput | undefined = undefined,
>(
  definition: Definition<C, I, O, A, SI, P, Q, W> & {
    readonly mapAccumBatch: ((
      state: NoInfer<A>,
      batch: Slices<Addresses<I>>,
      unit: Unit<StreamRoute.Params<C>>,
    ) => Step<readonly [A, X], E, R>) &
      (Exclude<keyof X, keyof O> extends never
        ? unknown
        : { readonly undeclaredOutput: Exclude<keyof X, keyof O> });
  },
): AccumulatingDeclaration<C, I, O, A, E, R, P, Q, AcceptedServices<W>>;
export function mapAccumBatch(definition: Parameters<typeof build>[1]): unknown {
  return build("mapAccumBatch", definition);
}
