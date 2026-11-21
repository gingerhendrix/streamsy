import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("ProcessOutputs/Input", "process-types/:id", {
  params: { id: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()("ProcessOutputs/Output", "process-output/:id", {
  params: { id: Schema.FiniteFromString },
  schema: Schema.String,
}) {}

export const legacyHandler = Projection.process({
  id: "legacy-handler",
  params: { id: Schema.FiniteFromString },
  inputs: { input: Input },
  outputs: { output: Output },
  commit: Commit.transaction,
  process: () => Effect.void,
});

export const typedHandler = Projection.process({
  id: "typed-handler",
  params: { id: Schema.FiniteFromString },
  inputs: { input: Input },
  outputs: { output: Output },
  commit: Commit.transaction,
  prepare: (_batch, unit, { outputs }) => {
    const id: number = unit.params.id;
    const output: ReturnType<typeof Output.at> = outputs.output;
    void id;

    return Effect.succeed(output.id);
  },
  process: (_batch, _unit, { prepared, outputs }) => {
    const id: string = prepared;
    const output: ReturnType<typeof Output.at> = outputs.output;
    void id;
    void output;

    return Effect.void;
  },
});

const atLeastOnce = {
  id: "at-least-once-output",
  params: { id: Schema.FiniteFromString },
  inputs: { input: Input },
  outputs: { output: Output },
  commit: Commit.atLeastOnce,
  process: () => Effect.void,
};

// @ts-expect-error process outputs require the transaction's SQL capability
Projection.process(atLeastOnce);

Projection.process({
  id: "missing-param",
  params: {},
  inputs: {},
  // @ts-expect-error output params must be projection params of the same type
  outputs: { output: Output },
  commit: Commit.transaction,
  process: () => Effect.void,
});

export const outputsOnly = Projection.process({
  id: "outputs-only",
  params: { id: Schema.FiniteFromString },
  inputs: { input: Input },
  outputs: { output: Output },
  commit: Commit.transaction,
  process: (_batch, _unit, { outputs }) => {
    const output: ReturnType<typeof Output.at> = outputs.output;
    void output;

    return Effect.void;
  },
});

class Routed extends Streamsy.json<Routed>()(
  "ProcessOutputs/Routed",
  "process-types/r/:id/:date/g/:gen",
  {
    params: { id: Schema.FiniteFromString, date: Schema.String, gen: Schema.FiniteFromString },
    schema: Schema.String,
  },
) {}

const routed = Projection.perGeneration(Routed, "gen", { routeBy: ["date"] });

Projection.process({
  id: "typed-routed",
  params: { id: Schema.FiniteFromString },
  inputs: {},
  outputs: { routed },
  commit: Commit.transaction,
  process: (_batch, _unit, { outputs }) => {
    const opening: Effect.Effect<
      ReturnType<typeof Routed.at>,
      import("@streamsy/projection").ProjectionFault,
      Routed
    > = outputs.routed.open({ date: "day" });

    // @ts-expect-error Routed params use their decoded types.
    void outputs.routed.open({ date: 1 });

    // @ts-expect-error Non-routed params are taken from the binding.
    void outputs.routed.open({ date: "day", id: 1 });

    return Effect.asVoid(opening);
  },
});

const mismatchedParameter = {
  id: "typed-routed-mismatch",
  params: { id: Schema.String },
  inputs: {},
  outputs: { routed },
  commit: Commit.transaction,
  process: () => Effect.void,
};

// @ts-expect-error Non-routed params must have matching decoded types.
Projection.process(mismatchedParameter);
