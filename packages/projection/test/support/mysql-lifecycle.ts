/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- Bun provisions each test host inside its fresh database scope. */
import { describe, expect, test } from "vitest";
import { Array, Cause, Effect, Exit, Layer, Option, Result, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { SqlStorage } from "@streamsy/storage";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionWakeup,
  Transaction,
} from "@streamsy/projection";
import {
  dialectClient,
  freshDatabase,
  sqlDialects,
} from "../../../storage/test/dialects/support.ts";

class Source extends Streamsy.json<Source>()("VB3/Source", "vb3/source", {
  params: {},
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()("VB3/Output", "vb3/output/:route/:generation", {
  params: { route: Schema.String, generation: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

const routed = (generation: number) =>
  Projection.process({
    id: "vb3-routed",
    generation,
    params: { tenant: Schema.String },
    inputs: { source: Source },
    outputs: { out: Projection.perGeneration(Output, "generation", { routeBy: ["route"] }) },
    commit: Commit.transaction,
    process: (batch, _unit, { outputs: { out } }) =>
      Effect.gen(function* () {
        const items = batch.source.items;

        if (!Array.isReadonlyArrayNonEmpty(items)) return;
        const { wire } = yield* Output;
        const address = yield* out.open({ route: "one" });
        yield* Streams.append(address, items).pipe(Effect.provideService(StreamsWriter, wire));
      }),
  });

const attachable = (id: string) =>
  Projection.process({
    id,
    params: {},
    inputs: { source: Source },
    commit: Commit.transaction,
    process: () => Effect.void,
  });

export const registerMysqlLifecycle = () => {
  const dialect = sqlDialects.mysql;
  const url = dialect.url;
  const group = Streamsy.group(Source, Output);

  describe.skipIf(url === undefined)("mysql projection lifecycle", () => {
    test("retires and forgets routed bindings with long params", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);

          const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
            Layer.provideMerge(SqlStorage.layer(group)),
            Layer.provideMerge(dialectClient(dialect, fresh)),
          );

          yield* Effect.gen(function* () {
            const params = { tenant: "x".repeat(600) };
            const first = routed(1).at(params);
            const second = routed(2).at(params);
            const source = yield* Source;
            const output = yield* Output;
            const store = yield* CheckpointStore;
            const routes = store.routes!;

            yield* source.create({});
            yield* source.append({}, [1]);
            yield* Projection.run(first);
            yield* Projection.run(second);

            const route = { route: "one", generation: 1 };
            const firstId = Output.at(route).id;
            const nextRoute = { ...route, generation: 2 };
            const secondId = Output.at(nextRoute).id;
            expect(yield* routes.list(first)).toEqual([{ output: "out", streamId: firstId }]);
            expect(yield* routes.list(second)).toEqual([{ output: "out", streamId: secondId }]);

            expect(yield* Projection.retire(routed(1), params, { generation: 1 })).toEqual({
              record: "retired",
              outputs: {},
              routed: { out: [{ id: firstId, result: "removed" }] },
            });
            expect(yield* routes.list(first)).toEqual([]);
            expect(yield* output.head(route).pipe(Effect.flip)).toMatchObject({
              _tag: "StreamNotFound",
            });

            yield* Projection.forget(second);
            expect(yield* routes.list(second)).toEqual([]);
            expect((yield* store.load(second)).record).toEqual(Option.none());
            expect((yield* output.collect(nextRoute)).items).toEqual([1]);
          }).pipe(Effect.provide(host));
        }).pipe(Effect.scoped),
      );
    }, 15_000);

    test("rejects a projection id over 255 code points at attach with a private SQL client", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);

          const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
            Layer.provideMerge(SqlStorage.layer(group)),
            Layer.provide(dialectClient(dialect, fresh)),
          );

          yield* Effect.gen(function* () {
            const maximum = "𐀀".repeat(255);
            yield* ProjectionWakeup.attach(attachable(maximum));

            const oversized = attachable(maximum + "x");

            const wakeup = yield* ProjectionWakeup.attach(oversized).pipe(
              Effect.asVoid,
              Effect.exit,
            );

            const process = yield* Layer.build(Projection.attach(oversized)).pipe(
              Effect.asVoid,
              Effect.exit,
            );

            for (const exit of [wakeup, process]) {
              expect(Exit.isFailure(exit)).toBe(true);

              if (Exit.isFailure(exit))
                expect(Result.getOrThrow(Cause.findDefect(exit.cause))).toMatchObject({
                  _tag: "StorageFault",
                  operation: "projection.attach",
                  retryable: false,
                  message: expect.stringContaining("255 characters"),
                });
            }
          }).pipe(Effect.provide(host));
        }).pipe(Effect.scoped),
      );
    });
  });
};
