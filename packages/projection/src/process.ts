/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { Effect } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Slices } from "./batch.ts";
import {
  build,
  type Common,
  type Inputs,
  type Addresses,
  type RunningDeclaration,
  type Committing,
} from "./declaration.ts";
import type { Unit } from "./unit.ts";

/** Run side effects once per unit. prepare runs at least once, outside the transaction. */
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
>(
  definition: Common<C, I> &
    Committing<void, P, Q> & {
      readonly prepare?: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Prepared, PE, PR>;
      readonly process: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
        prepared: Prepared,
      ) => Effect.Effect<void, E, R>;
    },
): RunningDeclaration<C, I, E | PE, R, void, P, Q | PR>;
export function process(definition: Parameters<typeof build>[1]): unknown {
  return build("process", definition);
}
