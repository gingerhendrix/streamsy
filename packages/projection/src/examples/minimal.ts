import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, Output, Projection } from "../index.ts";

export class Numbers extends Streamsy.json<Numbers>()("Minimal/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}
export class Doubled extends Streamsy.json<Doubled>()("Minimal/Doubled", "doubled", {
  params: {},
  schema: Schema.Finite,
}) {}

/** A per-entry map with outputs and offsets in one transaction. */
export const minimal = Effect.gen(function* () {
  const numbers = yield* Numbers;
  const doubled = yield* Doubled;
  yield* numbers.create({});
  yield* doubled.create({});
  yield* numbers.append({}, [1, 2, 3]);
  return yield* Projection.run(
    Projection.emit({
      id: "doubled",
      params: {},
      inputs: { input: Numbers },
      outputs: { output: Output.stream(Schema.Finite, { stream: Doubled }) },
      commit: Commit.transaction,
      emit: ({ item }) => ({ output: [item * 2] }),
    }).at({}),
    { limit: 10 },
  );
});
