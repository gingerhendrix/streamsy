import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", {
  schema: Schema.Struct({ text: Schema.String }),
});
const program = Effect.gen(function* () {
  yield* Streams.create(events);

  yield* Streams.append(events, [{ text: "hello" }]);

  const batches = yield* Streams.read(events).pipe(Stream.runCollect);
  const first = yield* Streams.follow(events).pipe(
    Streams.items,
    Stream.take(1),
    Stream.runCollect,
  );
  return { batches, first };
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
