/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- Bun provisions each test host inside its fresh database scope. */
import { describe, expect, test } from "vitest";
import {
  Array,
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Result,
  Schema,
} from "effect";
import { SqlClient, Statement } from "effect/sql";
import { CommitBoundary, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
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
  forkWaiting,
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

const routed = (generation: number, beforeSettle: Effect.Effect<void> = Effect.void) =>
  Projection.process({
    id: "vb3-routed",
    generation,
    params: { tenant: Schema.String },
    inputs: { source: Source },
    outputs: { out: Projection.perGeneration(Output, "generation", { routeBy: ["route"] }) },
    commit: Commit.transaction,
    process: (batch, unit, { outputs: { out } }) =>
      Effect.gen(function* () {
        const items = batch.source.items;

        if (!Array.isReadonlyArrayNonEmpty(items)) return;
        const { wire } = yield* Output;
        const route = unit.params.tenant.endsWith("two") ? "two" : "one";
        const address = yield* out.open({ route });
        yield* Streams.append(address, items).pipe(Effect.provideService(StreamsWriter, wire));
        yield* beforeSettle;
      }),
  });

// The field name makes the canonical JSON longer than 512 characters while
// the value still fits in a real input stream id.
const longField =
  "tenantIdentifierStoredInTheCanonicalBindingParamsForThisProjectionAndReadBackAfterTheDurableRunnerIsReattached";

const longParams = { [longField]: Schema.String };

class LongSource extends Streamsy.json<LongSource>()("VB3/LongSource", `vb3/long/:${longField}`, {
  params: longParams,
  schema: Schema.Finite,
}) {}

const longBinding = { [longField]: "x".repeat(480) };

const longProjection = (id: string, generation: number) =>
  Projection.process({
    id,
    generation,
    params: longParams,
    inputs: { source: LongSource },
    commit: Commit.transaction,
    process: () => Effect.void,
  });

const attachable = (id: string) =>
  Projection.process({
    id,
    params: {},
    inputs: { source: Source },
    commit: Commit.transaction,
    process: () => Effect.void,
  });

const dialect = sqlDialects.mysql;

const url = dialect.url;

const group = Streamsy.group(Source, Output, LongSource);

const projectionLayers = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(SqlStorage.layer(group)),
);

const projectionHost = (database: string) =>
  projectionLayers.pipe(
    Layer.provideMerge(dialectClient(dialect, database, { maxConnections: 1 })),
  );

const readOwnerKeys = (client: SqlClient.SqlClient) =>
  client<{ readonly owner_key: string }>`SELECT owner_key FROM streamsy_owner_records`;

export const registerMysqlLifecycle = () => {
  describe.skipIf(url === undefined)("mysql projection lifecycle", () => {
    test("forgets a routed binding whose first checkpoint committed after an earlier read", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const writer = yield* Layer.build(projectionHost(fresh));
          const reader = yield* Layer.build(projectionHost(fresh));
          const sql = Context.get(reader, SqlClient.SqlClient);
          const boundary = Context.get(reader, CommitBoundary);
          const store = Context.get(writer, CheckpointStore);
          const source = Context.get(writer, Source);
          const binding = routed(1).at({ tenant: "late-checkpoint" });
          const writerSql = Context.get(writer, SqlClient.SqlClient);

          yield* sql`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`;
          yield* source.create({});
          yield* source.append({}, [1]);

          const runWriter = yield* forkWaiting(
            Effect.gen(function* () {
              expect((yield* Projection.run(binding)).units).toBe(1);
              const written = yield* readOwnerKeys(writerSql);
              expect(Array.map(written, ({ owner_key }) => owner_key)).toEqual(
                expect.arrayContaining([expect.stringContaining("streamsy.projection.routed.v1")]),
              );
              expect((yield* store.load(binding)).record._tag).toBe("Some");
            }).pipe(Effect.provideContext(writer)),
          );

          yield* boundary.withTransaction(
            Effect.gen(function* () {
              expect(yield* readOwnerKeys(sql)).toEqual([]);
              yield* runWriter;
              yield* Projection.forget(binding).pipe(Effect.provideContext(reader));
            }),
          );

          const routes = store.routes!;
          const streamId = Output.at({ route: "one", generation: 1 }).id;
          expect((yield* store.load(binding)).record).toEqual(Option.none());
          expect(yield* routes.list(binding)).toEqual([{ output: "out", streamId }]);
          expect(yield* routes.state(binding, "out", streamId)).toBe("forgotten");
          expect(
            (yield* Context.get(writer, Output).collect({ route: "one", generation: 1 })).items,
          ).toEqual([1]);
        }).pipe(Effect.scoped),
      );
    }, 15_000);

    test("lists bindings without blocking a new binding at READ COMMITTED", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const writer = yield* Layer.build(projectionHost(fresh));
          const reader = yield* Layer.build(projectionHost(fresh));
          const source = Context.get(writer, Source);
          const boundary = Context.get(reader, CommitBoundary);
          const sql = Context.get(reader, SqlClient.SqlClient);
          const projection = routed(1);
          const first = { tenant: "one" };
          const second = { tenant: "two" };

          yield* sql`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`;
          yield* source.create({});
          yield* source.append({}, [1]);
          yield* Projection.run(projection.at(first)).pipe(Effect.provideContext(writer));

          const runWriter = yield* forkWaiting(
            Projection.run(projection.at(second)).pipe(Effect.provideContext(writer)),
          );

          const list = Projection.bindings(projection).pipe(Effect.provideContext(reader));

          yield* boundary.withTransaction(
            Effect.gen(function* () {
              expect(yield* list).toEqual([first]);
              expect((yield* runWriter).units).toBe(1);
              expect(yield* list).toEqual(expect.arrayContaining([first, second]));
            }),
          );
        }).pipe(Effect.scoped),
      );
    }, 15_000);

    test("retires and forgets routed bindings with long params", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);

          const host = projectionHost(fresh);

          yield* Effect.gen(function* () {
            const common = "x".repeat(600);
            const params = { tenant: common + "one" };
            const first = routed(1).at(params);
            const second = routed(2).at(params);
            const neighbour = routed(1).at({ tenant: common + "two" });
            const source = yield* Source;
            const output = yield* Output;
            const store = yield* CheckpointStore;
            const routes = store.routes!;

            yield* source.create({});
            yield* source.append({}, [1]);
            yield* Projection.run(first);
            yield* Projection.run(second);
            yield* Projection.run(neighbour);

            const route = { route: "one", generation: 1 };
            const firstId = Output.at(route).id;
            const nextRoute = { ...route, generation: 2 };
            const secondId = Output.at(nextRoute).id;
            const otherRoute = { ...route, route: "two" };
            const neighbourRecord = { output: "out", streamId: Output.at(otherRoute).id };
            expect(yield* routes.list(first)).toEqual([{ output: "out", streamId: firstId }]);
            expect(yield* routes.list(second)).toEqual([{ output: "out", streamId: secondId }]);
            expect(yield* routes.list(neighbour)).toEqual([neighbourRecord]);

            expect(yield* Projection.retire(routed(1), params, { generation: 1 })).toEqual({
              record: "retired",
              streams: [{ output: "out", id: firstId, result: "removed" }],
            });
            expect(yield* routes.list(first)).toEqual([]);
            expect(yield* routes.list(neighbour)).toEqual([neighbourRecord]);
            expect((yield* output.collect(otherRoute)).items).toEqual([1]);
            expect(yield* output.head(route).pipe(Effect.flip)).toMatchObject({
              _tag: "StreamNotFound",
            });

            yield* Projection.forget(second);
            expect(yield* routes.state(second, "out", secondId)).toBe("forgotten");
            expect((yield* store.load(second)).record).toEqual(Option.none());
            expect((yield* output.collect(nextRoute)).items).toEqual([1]);
          }).pipe(Effect.provide(host));
        }).pipe(Effect.scoped),
      );
    }, 15_000);

    test("retries a deadlocked retire after the binding's first routed unit settles", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const host = () => Layer.build(projectionHost(fresh));
          const unitHost = yield* host();
          const retireHost = yield* host();
          const unitSql = Context.get(unitHost, SqlClient.SqlClient);
          const retireSql = Context.get(retireHost, SqlClient.SqlClient);
          const unitConnection = yield* unitSql`SELECT CONNECTION_ID() AS id`;
          const retireConnection = yield* retireSql`SELECT CONNECTION_ID() AS id`;
          expect(unitConnection).not.toEqual(retireConnection);

          const routeOpened = yield* Deferred.make<void>();
          const retireListing = yield* Deferred.make<void>();

          const spec = routed(
            1,
            Deferred.succeed(routeOpened, undefined).pipe(
              Effect.andThen(Deferred.await(retireListing)),
            ),
          );

          const params = { tenant: "race" };
          const binding = spec.at(params);
          const source = Context.get(unitHost, Source);
          const store = Context.get(retireHost, CheckpointStore);
          const output = Context.get(retireHost, Output);
          const route = { route: "one", generation: 1 };

          yield* source.create({});
          yield* source.append({}, [1]);

          const unit = yield* Projection.run(binding).pipe(
            Effect.provideContext(unitHost),
            Effect.exit,
            Effect.forkScoped,
          );

          yield* Deferred.await(routeOpened);

          const retire = Projection.retire(spec, params, { generation: 1 }).pipe(
            Effect.provideContext(retireHost),
          );

          const retired = yield* retire.pipe(
            Effect.provideService(Statement.CurrentTransformer, (statement) => {
              const [query] = statement.compile();

              // Retire has already written its tombstone. The first unit
              // holds its route and can now try to write its checkpoint.
              return query.startsWith("SELECT owner_key, full_key, owner_value")
                ? Deferred.succeed(retireListing, undefined).pipe(Effect.as(statement))
                : Effect.succeed(statement);
            }),
            Effect.exit,
          );

          const settled = yield* Fiber.join(unit);
          expect(settled._tag).toBe("Success");
          expect((yield* store.load(binding)).record._tag).toBe("Some");
          expect((yield* output.collect(route)).items).toEqual([1]);
          expect(retired._tag).toBe("Failure");

          if (Exit.isFailure(retired)) {
            const fault = Option.getOrThrow(Cause.findErrorOption(retired.cause));
            expect(fault).toMatchObject({
              _tag: "ProjectionFault",
              reason: "storage-failure",
              retryable: true,
              cause: { retryable: true, cause: { reason: { cause: { errno: 1213 } } } },
            });
            yield* Effect.logInfo(
              "MySQL error 1213 reached Projection as a retryable retire failure; first unit succeeded",
              fault,
            );
          }

          const retry = yield* retire;
          yield* Effect.logInfo("MySQL retire retry succeeded", retry);
          expect(retry).toEqual({
            record: "retired",
            streams: [{ output: "out", id: Output.at(route).id, result: "removed" }],
          });
          expect(yield* store.load(binding).pipe(Effect.flip)).toMatchObject({
            reason: "retired-generation",
          });
          expect(yield* store.routes!.list(binding)).toEqual([]);
          expect(yield* output.head(route).pipe(Effect.flip)).toMatchObject({
            _tag: "StreamNotFound",
          });
        }).pipe(Effect.scoped),
      );
    }, 15_000);

    test("rolls back retire and forget when a legacy routed key cannot be listed", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const ctx = yield* Layer.build(projectionHost(fresh));
          const sql = Context.get(ctx, SqlClient.SqlClient);
          const source = Context.get(ctx, Source);
          const store = Context.get(ctx, CheckpointStore);
          const spec = routed(1);
          const params = { tenant: "x".repeat(600) };
          const binding = spec.at(params);

          yield* source.create({});
          yield* source.append({}, [1]);
          yield* Projection.run(binding).pipe(Effect.provideContext(ctx));
          expect(yield* store.routes!.list(binding)).toHaveLength(1);

          const prefix = '["streamsy.projection.routed.v1",%';
          yield* sql`UPDATE streamsy_owner_records SET full_key = NULL WHERE owner_key LIKE ${prefix}`;

          const legacy =
            yield* sql`SELECT owner_key, full_key FROM streamsy_owner_records WHERE owner_key LIKE ${prefix}`;

          expect(legacy).toEqual([
            {
              owner_key: expect.stringMatching(/#[0-9a-f]{64}$/u),
              full_key: null,
            },
          ]);

          const snapshot = Effect.gen(function* () {
            return {
              records: yield* sql`SELECT * FROM streamsy_owner_records ORDER BY owner_key`,
              streams: yield* sql`SELECT * FROM streamsy_streams ORDER BY stream_id`,
              messages: yield* sql`SELECT * FROM streamsy_messages ORDER BY stream_id, msg_offset`,
            };
          });

          const before = yield* snapshot;
          const checkpoint = yield* store.load(binding);
          expect(checkpoint.record._tag).toBe("Some");

          for (const call of [
            Projection.retire(spec, params, { generation: 1 }).pipe(Effect.asVoid),
            Projection.forget(binding),
          ]) {
            const fault = yield* call.pipe(Effect.provideContext(ctx), Effect.flip);
            expect(fault).toMatchObject({
              _tag: "ProjectionFault",
              reason: "unsupported-composition",
              retryable: false,
              message: expect.stringContaining("1 legacy hashed keys"),
            });
            expect(yield* snapshot).toEqual(before);
            expect(yield* store.load(binding)).toEqual(checkpoint);
          }
        }).pipe(Effect.scoped),
      );
    });

    test("a reattached durable runner consumes a long mark written by a real input commit", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const ctx = yield* Layer.build(projectionHost(fresh));
          const source = Context.get(ctx, LongSource);
          const store = Context.get(ctx, CheckpointStore);
          const marks = Context.get(ctx, ProjectionMarks);
          const spec = longProjection("vb3-durable-long", 1);
          const binding = spec.at(longBinding);
          const canonical = `{"${longField}":"${longBinding[longField]}"}`;

          expect(canonical.length).toBeGreaterThan(512);
          yield* source.create(longBinding);

          const appended = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* ProjectionWakeup.attach(spec, { durable: true });

              return yield* source.append(longBinding, [1]);
            }).pipe(Effect.provideContext(ctx)),
          );

          expect((yield* store.load(binding)).record._tag).toBe("None");
          expect((yield* marks.read(spec.id, [canonical])).get(canonical)?.params).toBe(canonical);

          yield* Effect.scoped(
            Effect.gen(function* () {
              const runner = yield* ProjectionWakeup.attach(spec, { durable: true });
              expect(yield* runner.run).toEqual({ more: false });
            }).pipe(Effect.provideContext(ctx)),
          );

          const loaded = yield* store.load(binding);
          expect(Option.getOrThrow(loaded.record).inputs.source).toBe(appended.offset);
          expect((yield* marks.read(spec.id, [canonical])).size).toBe(0);
        }).pipe(Effect.scoped),
      );
    });

    test("discovers a stored binding from another generation at MySQL startup", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const ctx = yield* Layer.build(projectionHost(fresh));
          const source = Context.get(ctx, LongSource);
          const store = Context.get(ctx, CheckpointStore);
          const marks = Context.get(ctx, ProjectionMarks);
          const original = longProjection("vb3-startup", 1);
          const current = longProjection(original.id, 2);

          yield* source.create(longBinding);
          const appended = yield* source.append(longBinding, [1]);
          yield* Projection.run(original.at(longBinding)).pipe(Effect.provideContext(ctx));
          expect((yield* store.load(current.at(longBinding))).record._tag).toBe("None");
          expect(yield* marks.due(current.id, Number.MAX_SAFE_INTEGER, 10)).toEqual([]);

          yield* Effect.scoped(
            Effect.gen(function* () {
              const runner = yield* ProjectionWakeup.attach(current, { durable: true });
              expect(yield* runner.run).toEqual({ more: false });
            }).pipe(Effect.provideContext(ctx)),
          );

          for (const spec of [original, current]) {
            const { record } = yield* store.load(spec.at(longBinding));
            expect(Option.getOrThrow(record).inputs.source).toBe(appended.offset);
          }
        }).pipe(Effect.scoped),
      );
    });

    test("limits IDs to 255 code points on both durable attach hosts with a private SQL client", async () => {
      if (url === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);

          const host = projectionLayers.pipe(Layer.provide(dialectClient(dialect, fresh)));

          yield* Effect.gen(function* () {
            const maximum = "𐀀".repeat(255);
            yield* ProjectionWakeup.attach(attachable(maximum), { durable: true });
            yield* Layer.build(Projection.attach(attachable(maximum), { durable: true }));

            const oversized = attachable(maximum + "x");

            const wakeup = yield* ProjectionWakeup.attach(oversized, { durable: true }).pipe(
              Effect.asVoid,
              Effect.exit,
            );

            const process = yield* Layer.build(
              Projection.attach(oversized, { durable: true }),
            ).pipe(Effect.asVoid, Effect.exit);

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

            yield* ProjectionWakeup.attach(oversized, { durable: false });
            yield* Layer.build(Projection.attach(oversized, { durable: false }));
          }).pipe(Effect.provide(host));
        }).pipe(Effect.scoped),
      );
    });
  });
};
