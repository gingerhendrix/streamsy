import * as FamilyFixture from "./families.ts";
import { Effect, Schema, Stream } from "effect";
import { StreamAddress } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { Checkpoints, Projection, ProjectionFault, type State } from "@streamsy/projection";

export const input = FamilyFixture.json("projection-input", { schema: Schema.Finite });
export const output = FamilyFixture.json("projection-output", { schema: Schema.Finite });

/** Stateless atomic filter: positive items are copied to `output` inside the transaction. */
export const positives = Projection.atomic({
  id: "positives",
  params: {},
  inputs: { input },
  process: (batch) =>
    Effect.gen(function* () {
      const kept = batch.input.items.filter((item) => item > 0);
      if (kept.length > 0) yield* Streams.append(output, kept);
    }),
}).at({});
export type Services =
  | Checkpoints
  | State
  | import("@streamsy/core").Streamsy.Wildcard
  | StreamsReader
  | StreamsWriter;

export const initialize = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.create(output);
  yield* Streams.append(input, [1, -1, 3]);
});
export const readAll = <A>(address: StreamAddress.StreamAddress<A>) =>
  Streams.read(address).pipe(Streams.items, Stream.runCollect);
export const inspect = Effect.gen(function* () {
  const owner = yield* Checkpoints;
  return {
    output: yield* readAll(output),
    loaded: yield* owner.load(Projection.key(positives)),
  };
});

/** Fault injected after the handler's real append, at the checkpoint save. */
export const failAtSave = Effect.gen(function* () {
  const owner = yield* Checkpoints;
  const failing = Checkpoints.of({
    ...owner,
    save: () =>
      Effect.fail(
        new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "after-handler",
        }),
      ),
  });
  return yield* Projection.run(positives).pipe(
    Effect.provideService(Checkpoints, failing),
    Effect.result,
  );
});

/** Same composition scenario on memory, Bun SQLite and local Durable Object SQLite. */
export const composition = Effect.gen(function* () {
  yield* initialize;
  const first = yield* Projection.run(positives, { limit: 1 });
  const before = yield* inspect;
  const failed = yield* failAtSave;
  const after = yield* inspect;
  const final = yield* Projection.run(positives);
  const restart = yield* Projection.run(positives);
  return { first, before, failed: failed._tag, after, final, restart, stored: yield* inspect };
});
