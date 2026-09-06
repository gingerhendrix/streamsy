import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.String });
const program = Effect.gen(function* () {
  const created = yield* Streams.create(events);
  if (created.status !== "created" && created.status !== "exists") return created;
  const appended = yield* Streams.append(events, ["hello"]);
  if (appended.status !== "appended") return appended;
  return yield* Streams.read(events).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
