import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "../index.ts";

export class Numbers extends Streamsy.json<Numbers>()("Minimal/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}
export class Doubled extends Streamsy.json<Doubled>()("Minimal/Doubled", "doubled", {
  params: {},
  schema: Schema.Finite,
}) {}

/** A stateless fused map inside the checkpoint transaction. */
export const minimal = Effect.gen(function* () {
  const numbers = yield* Numbers;
  const doubled = yield* Doubled;
  yield* numbers.create({});
  yield* doubled.create({});
  yield* numbers.append({}, [1, 2, 3]);
  return yield* Projection.run(
    Projection.fused({
      id: "doubled",
      inputs: { input: Numbers.at({}) },
      process: (batch) =>
        Effect.flatMap(Doubled, (output) =>
          output.append(
            {},
            batch.input.items.map((n) => n * 2),
          ),
        ),
    }),
    { limit: 10 },
  );
});
