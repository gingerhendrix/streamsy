import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry, Slices } from "./batch.ts";
import type { Accumulated } from "./commit.ts";
import {
  build,
  type Common,
  type Inputs,
  type Addresses,
  type AccumulatingDeclaration,
  type Committing,
  type OutputsWithin,
} from "./declaration.ts";
import type { RoutedMap } from "./output-internal.ts";
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
> = Common<C, I> &
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
  E = never,
  R = never,
  P = never,
  Q = never,
>(
  definition: Definition<C, I, O, A, SI, P, Q> & {
    readonly mapAccum: (
      state: NoInfer<A>,
      entry: Entry<Addresses<I>>,
      unit: Unit<StreamRoute.Params<C>>,
    ) => Step<readonly [A, Emits<O>], E, R>;
  },
): AccumulatingDeclaration<C, I, O, A, SI, E, R, P, Q>;
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
  E = never,
  R = never,
  P = never,
  Q = never,
>(
  definition: Definition<C, I, O, A, SI, P, Q> & {
    readonly mapAccumBatch: (
      state: NoInfer<A>,
      batch: Slices<Addresses<I>>,
      unit: Unit<StreamRoute.Params<C>>,
    ) => Step<readonly [A, Emits<O>], E, R>;
  },
): AccumulatingDeclaration<C, I, O, A, SI, E, R, P, Q>;
export function mapAccumBatch(definition: Parameters<typeof build>[1]): unknown {
  return build("mapAccumBatch", definition);
}
