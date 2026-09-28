import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry } from "./batch.ts";
import type { Folded } from "./commit.ts";
import type { CheckpointStore } from "./checkpoint-store.ts";
import {
  build,
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
>(
  definition: Common<C, I> & {
    readonly state: StateDefinition<A, SI, StreamRoute.Params<C>>;
    readonly commit?: Committing<Folded<NoInfer<A>>, P, Q>["commit"];
    readonly fold: (
      state: NoInfer<A>,
      entry: Entry<Addresses<I>>,
      unit: Unit<StreamRoute.Params<C>>,
    ) => Step<A, E, R>;
  },
): StatefulDeclaration<C, I, A, SI, E, R, Folded<A>, P, Q>;
export function fold(definition: Parameters<typeof build>[1]): unknown {
  return build("fold", definition);
}
