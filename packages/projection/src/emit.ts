/* oxlint-disable anti-slop/no-unknown-returns -- Only the implementation signature is erased; callers see the checked generic overload above it. */
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry } from "./batch.ts";
import type { Emitted } from "./commit.ts";
import {
  build,
  type Common,
  type Inputs,
  type Addresses,
  type OutputDeclaration,
  type Committing,
  type OutputsWithin,
} from "./declaration.ts";
import type { RoutedMap } from "./output-internal.ts";
import type { Step, Emits } from "./step.ts";
import type { Unit } from "./unit.ts";

/** Emit zero or more output items for each input entry, in entry order. */
export function emit<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  const O extends RoutedMap,
  X extends Emits<O>,
  E = never,
  R = never,
  P = never,
  Q = never,
>(
  definition: Common<C, I> &
    Committing<Emitted<NoInfer<O>>, P, Q> & {
      readonly outputs: O & OutputsWithin<C, O>;
      readonly emit: ((
        entry: Entry<Addresses<I>>,
        unit: Unit<StreamRoute.Params<C>>,
      ) => Step<X, E, R>) &
        (Exclude<keyof X, keyof O> extends never
          ? unknown
          : { readonly undeclaredOutput: Exclude<keyof X, keyof O> });
    },
): OutputDeclaration<C, I, O, E, R, Emitted<O>, P, Q>;
export function emit(definition: Parameters<typeof build>[1]): unknown {
  return build("emit", definition);
}
