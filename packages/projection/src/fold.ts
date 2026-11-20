/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { AcceptedOutput } from "./accepted.ts";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry } from "./batch.ts";
import type { Folded } from "./commit.ts";
import type { CheckpointStore } from "./checkpoint-store.ts";
import {
  build,
  type AcceptedServices,
  type AcceptedResult,
  type Common,
  type Inputs,
  type Addresses,
  type StatefulDeclaration,
  type Committing,
} from "./declaration.ts";
import type { StateDefinition } from "./state.ts";
import type { Step } from "./step.ts";
import type { Unit } from "./unit.ts";

/** Fold one entry at a time. The default Commit saves state and offsets in one write. */
export function fold<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  A,
  SI,
  E = never,
  R = never,
  P = never,
  Q = CheckpointStore,
  W extends AcceptedOutput | undefined = undefined,
  HE = never,
  HR = never,
>(
  definition: Common<C, I, W, HE, HR> &
    ([W] extends [undefined]
      ? Partial<Committing<AcceptedResult<Folded<NoInfer<A>>, W>, P, Q>>
      : Committing<AcceptedResult<Folded<NoInfer<A>>, W>, P, Q>) & {
      readonly state: StateDefinition<A, SI, StreamRoute.Params<C>>;
      readonly fold: (
        state: NoInfer<A>,
        entry: Entry<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Step<A, E, R>;
    },
): StatefulDeclaration<C, I, A, E, R, Folded<A>, P, Q, AcceptedServices<W>, HE, HR>;
export function fold(definition: Parameters<typeof build>[1]): unknown {
  return build("fold", definition);
}
