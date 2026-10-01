import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./families.ts";
import { Array, Effect, Schema, Stream } from "effect";
import { StreamAddress } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Projection,
  ProjectionFault,
  type Transaction,
} from "@streamsy/projection";

export const input = FamilyFixture.json("projection-input", { schema: Schema.Finite });
export const output = FamilyFixture.json("projection-output", { schema: Schema.Finite });

/** Stateless transactional filter: positive items are copied to `output` inside the transaction. */
export const positives = Projection.process({
  commit: Commit.transaction,
  id: "positives",
  params: {},
  inputs: { input },
  process: (batch) =>
    Effect.gen(function* () {
      const kept = batch.input.items.filter((item) => item > 0);
      if (Array.isReadonlyArrayNonEmpty(kept)) yield* Streams.append(output, kept);
    }),
}).at({});
export type Services =
  | CheckpointStore
  | Transaction
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
  const owner = yield* CheckpointStore;
  return {
    output: yield* readAll(output),
    loaded: yield* owner.load(Projection.key(positives)),
  };
});

/** Fault injected after the handler's real append, at the checkpoint save. */
export const failAtSave = Effect.gen(function* () {
  const owner = yield* CheckpointStore;
  const failing = CheckpointStore.of({
    ...owner,
    compareAndSet: () =>
      Effect.fail(
        new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "after-handler",
        }),
      ),
  });
  return yield* Projection.run(positives).pipe(
    Effect.provideService(CheckpointStore, failing),
    Effect.result,
  );
});

/** Same composition scenario on Bun SQLite and local Durable Object SQLite. */
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
