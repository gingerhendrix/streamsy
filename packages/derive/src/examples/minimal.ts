import { Effect, Schema } from "effect";
import { StreamRef, Streams } from "@streamsy/core";
import { Projection, StreamSink, StreamSource } from "../index.ts";

const input = StreamRef.json("numbers", { schema: Schema.Finite });
const output = StreamRef.json("running-totals", { schema: Schema.Finite });

/** Host supplies one fused graph; no runtime or background work is hidden here. */
export const minimal = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.create(output);
  yield* Streams.append(input, [1, 2, 3]);
  const projection = Projection.make({
    id: "running-total",
    version: 1,
    source: yield* StreamSource.make(input),
    sink: yield* StreamSink.make(output),
    initial: 0,
    stateSchema: Schema.fromJsonString(Schema.Finite),
    step: (state, item) => ({ state: state + item, outputs: [state + item] }),
  });
  return yield* Projection.catchUp(projection, { boundaries: 10, items: 100 });
});
