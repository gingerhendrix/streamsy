/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { Effect } from "effect";
import type { SqlClient } from "effect/sql";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Slices } from "./batch.ts";
import {
  build,
  type Common,
  type Inputs,
  type Addresses,
  type RunningDeclaration,
  type Committing,
  type OutputDeclaration,
  type OutputsWithin,
} from "./declaration.ts";
import type { RoutedMap } from "./outputs.ts";
import type { Unit } from "./unit.ts";

/** The third argument of `process`. */
export interface ProcessContext<Prepared = void> {
  readonly prepared: Prepared;
}

/** The bound address of each declared output, in the context of `prepare` and `process`. */
export interface OutputsContext<O extends RoutedMap> {
  readonly outputs: Addresses<O>;
}

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
      readonly outputs?: never;
      readonly prepare?: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Effect.Effect<Prepared, PE, PR>;
      readonly process: (
        batch: Slices<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
        context: ProcessContext<Prepared>,
      ) => Effect.Effect<void, E, R>;
    },
): RunningDeclaration<C, I, E | PE, R, void, P, Q | PR>;
// The outputs overload is last, so the compiler reports a mistake in `outputs` against it.
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
>(
  definition: Common<C, I> &
    Committing<void, SqlClient.SqlClient, Q> & {
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
      ) => Effect.Effect<void, E, R>;
    },
): OutputDeclaration<C, I, O, E | PE, R, void, SqlClient.SqlClient, Q | PR>;
export function process(definition: Parameters<typeof build>[1]): unknown {
  return build("process", definition);
}
