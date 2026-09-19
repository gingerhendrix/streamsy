import { Effect, Schema } from "effect";
import { StreamRef, Streams } from "@streamsy/core";
import { Projection } from "../index.ts";

const numbers = StreamRef.json("numbers", { schema: Schema.Finite });
const doubled = StreamRef.json("doubled", { schema: Schema.Finite });

/** A stateless fused map: each doubled item is appended inside the checkpoint transaction. */
export const minimal = Effect.gen(function* () {
  yield* Streams.create(numbers);
  yield* Streams.create(doubled);
  yield* Streams.append(numbers, [1, 2, 3]);
  const projection = Projection.make({
    id: "doubled",
    input: numbers,
    process: (batch) =>
      Streams.append(
        doubled,
        batch.input.items.map((n) => n * 2),
      ),
  });
  return yield* Projection.run(projection, { limit: 10 });
});
