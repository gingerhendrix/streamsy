import { Effect, Schema, Stream } from "effect";
import { StreamAddress, MemoryStorage } from "@streamsy/core";
import { Streams } from "@streamsy/core/protocol";

const events = StreamAddress.json("events", {
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
await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layerProtocol())));
