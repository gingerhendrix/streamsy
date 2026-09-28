import type { Effect } from "effect";
import type { Streamsy } from "@streamsy/core";
import type { Commit } from "./commit.ts";
import type { InputMap, Slices } from "./batch.ts";
import type { Identity } from "./checkpoint-store.ts";
import type { BoundState } from "./state.ts";
import type { Unit } from "./unit.ts";

/** Common bound declaration consumed by the kernel. */
export interface Executable<
  I extends InputMap,
  E,
  R,
  Q,
  Result,
  P = never,
  S = unknown,
  OR = never,
> extends Identity {
  readonly inputs: I;
  readonly state?: BoundState<S>;
  readonly streams?: Readonly<Record<string, Streamsy.Address<unknown, OR>>>;
  readonly commit: Commit<Result, P, Q>;
  readonly prepare?: (batch: Slices<I>, unit: Unit) => Effect.Effect<unknown, E, Q>;
  readonly process: (
    batch: Slices<I>,
    unit: Unit,
    encoded?: string,
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Internal transport of the declaration-typed prepare result.
    prepared?: unknown,
  ) => Effect.Effect<Result, E, R | P>;
}
