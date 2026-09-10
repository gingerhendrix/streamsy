import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.String });
const program = Effect.gen(function* () {
  yield* Streams.create(events);

  yield* Streams.append(events, ["hello"]);

  return yield* Streams.read(events).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
