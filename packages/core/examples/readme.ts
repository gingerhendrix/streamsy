import { Effect, Schema, Stream } from "effect";
import { Memory, StreamFamily } from "@streamsy/core";

class Events extends StreamFamily.json<Events>()("Readme/Events", "events", {
  params: {},
  schema: Schema.String,
}) {}
const program = Effect.gen(function* () {
  const events = yield* Events;
  yield* events.create({});
  yield* events.append({}, ["hello"]);
  return yield* events.read({}).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Memory.layer({}, Events))));
