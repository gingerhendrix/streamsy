/* oxlint-disable eslint/no-underscore-dangle -- Effect results and stream refs use public tagged variants. */
import { Effect, Option, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";
import { Commit, DeriveFault, Projection, StreamSink, StreamSource } from "../src/index.ts";

export const input = StreamRef.json("derive-input", { schema: Schema.Finite });
export const output = StreamRef.json("derive-output", { schema: Schema.Finite });
export const definition = Effect.gen(function* () {
  return Projection.make({
    id: "sum",
    version: 1,
    source: yield* StreamSource.make(input),
    sink: yield* StreamSink.make(output),
    initial: 0,
    stateSchema: Schema.fromJsonString(Schema.Finite),
    step: (state, item) => ({ state: state + item, outputs: item > 0 ? [state + item] : [] }),
  });
});
export const initialize = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.create(output);
  yield* Streams.append(input, [1, -1, 3]);
});
export const inspect = Effect.gen(function* () {
  const owner = yield* Commit;
  return {
    output: yield* Streams.read(output).pipe(Streams.items, Stream.runCollect),
    checkpoint: Option.getOrThrow(yield* owner.checkpoints.load("sum")),
    state: Option.getOrThrow(yield* owner.states.load("sum")),
  };
});

/** Fault injected after the real sink append, before state/checkpoint writes. */
export const failAfterSink = Effect.gen(function* () {
  const owner = yield* Commit;
  const failing = Commit.of({
    ...owner,
    states: {
      ...owner.states,
      save: () =>
        Effect.fail(new DeriveFault({ reason: "storage-failure", message: "after-sink" })),
    },
  });
  return yield* Effect.gen(function* () {
    const projection = yield* definition;
    return yield* Projection.catchUp(projection);
  }).pipe(Effect.provideService(Commit, failing), Effect.result);
});

/** Same composition scenario on Bun and real local Durable Object SQLite. */
export const composition = Effect.gen(function* () {
  yield* initialize;
  const projection = yield* definition;
  const first = yield* Projection.catchUp(projection, { items: 1 });
  const before = yield* inspect;
  const failed = yield* failAfterSink;
  const after = yield* inspect;
  const final = yield* Projection.catchUp(projection);
  const restart = yield* definition.pipe(Effect.flatMap(Projection.catchUp));
  return { first, before, failed: failed._tag, after, final, restart, stored: yield* inspect };
});
