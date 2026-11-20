import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, Projection, type Emits } from "@streamsy/projection";
import {
  Accepted,
  Facts,
  Numbers,
  Rows,
  accepted,
  declaration,
  output,
} from "../support/generation-outputs.ts";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

const state = Projection.perGeneration(Rows, "rev");

export type EmitsUnchanged = Assert<
  Same<
    Emits<{ numbers: typeof output; state: typeof state }>,
    Emits<{ numbers: typeof Numbers; state: typeof Rows }>
  >
>;

const process = Projection.process({
  ...declaration,
  id: "typed-generation-process",
  outputs: { numbers: output, state },
  accepted,
  commit: Commit.transaction,
  process: (batch, unit, { outputs }) => {
    const checks: [
      Assert<Same<typeof outputs.numbers, ReturnType<typeof Numbers.at>>>,
      Assert<Same<typeof outputs.state, ReturnType<typeof Rows.at>>>,
      Assert<Same<(typeof batch.facts.items)[number], { readonly id: string; readonly n: number }>>,
      Assert<Same<typeof unit.params.workspace, string>>,
    ] = [true, true, true, true];

    return Effect.asVoid(Effect.succeed(checks));
  },
});

const bound = process.at({ workspace: "one" });

export const run = Projection.run(bound);

export type ServicesRequired = Assert<
  Numbers | Rows | Accepted | Facts extends Effect.Services<typeof run> ? true : false
>;

export const initialized = Projection.createOutputs(bound);

export type CreateServices = Assert<
  Same<Effect.Services<typeof initialized>, Numbers | Rows | Accepted>
>;

// @ts-expect-error the generation param decodes to a number
Projection.perGeneration(Numbers, "workspace");

// @ts-expect-error the generation param must exist on the route
Projection.perGeneration(Numbers, "missing");

Projection.emit({
  id: "generation-overlap",
  params: { ...declaration.params, rev: Schema.FiniteFromString },
  inputs: {},
  // @ts-expect-error an output generation param cannot also come from projection params
  outputs: { numbers: output },
  commit: Commit.producer,
  emit: () => ({}),
});

Projection.emit({
  id: "generation-missing-workspace",
  params: {},
  inputs: {},
  // @ts-expect-error unbound params must still come from the projection
  outputs: { numbers: output },
  commit: Commit.producer,
  emit: () => ({}),
});

Projection.process({
  id: "generation-wrapped-input",
  params: declaration.params,
  // @ts-expect-error inputs do not accept a generation wrapper
  inputs: { numbers: output },
  commit: Commit.transaction,
  process: () => Effect.void,
});

Projection.process({
  ...declaration,
  id: "generation-accepted-overlap",
  params: { ...declaration.params, epoch: Schema.FiniteFromString },
  // @ts-expect-error an accepted generation param cannot also come from projection params
  accepted,
  commit: Commit.transaction,
  process: () => Effect.void,
});

Projection.process({
  ...declaration,
  id: "generation-accepted-json",
  // @ts-expect-error accepted still requires a wake class
  accepted: output,
  commit: Commit.transaction,
  process: () => Effect.void,
});

Projection.wake({
  id: "generation-wake-output",
  params: declaration.params,
  input: Facts,
  // @ts-expect-error the wake output does not accept a generation wrapper
  output: accepted,
});

class Wake extends Streamsy.wake<Wake>()("G1Types/Wake", "g1/wake/:workspace", {
  params: declaration.params,
}) {}

export const wake = Projection.wake({
  id: "generation-wake-accepted",
  params: declaration.params,
  input: Facts,
  output: Wake,
  accepted,
});
