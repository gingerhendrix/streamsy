import { expect, test } from "bun:test";
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { SqlClient } from "effect/unstable/sql";
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient";
import { Streamsy } from "@streamsy/core";
import { Protocol, Streams } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as ProjectionSql from "@streamsy/projection/sql";

class Input extends Streamsy.json<Input>()("transaction/Input", "transaction/input", {
  params: {},
  schema: Schema.Number,
}) {}
class Sink extends Streamsy.json<Sink>()("transaction/Sink", "transaction/sink", {
  params: {},
  schema: Schema.Number,
}) {}
const graph = Memory.layerMemory();
const memory = Protocol.bind(graph, Streamsy.group(Input, Sink)).pipe(Layer.provideMerge(graph));
const seed = Effect.gen(function* () {
  const input = yield* Input;
  yield* input.create({});
  yield* input.append({}, [1]);
});
const sqlProcess = Projection.process({
  commit: Commit.transaction,
  id: "sql",
  params: {},
  inputs: { input: Input },
  process: () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("INSERT INTO effects (value) VALUES (1)");
    }),
}).at({});

test("memory SQL requests fail as unsupported composition; other defects remain defects", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const result = yield* Projection.run(sqlProcess).pipe(Effect.result);
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { phase: "process", reason: "unsupported-composition" },
      });
      expect((yield* (yield* CheckpointStore).load(sqlProcess)).token).toBe("0");
      const defect = new Error("different defect");
      const broken = { ...sqlProcess, process: () => Effect.die(defect) };
      const exit = yield* Projection.run(broken).pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(defect);
    }).pipe(Effect.provide(memory)),
  ));

test("SQL from Transaction is sufficient and rolls back with the checkpoint", () => {
  const sqlLayer = SqliteClient.layer({ filename: ":memory:" });
  const host = Layer.merge(
    ProjectionSql.layer.pipe(Layer.provideMerge(sqlLayer)),
    Protocol.bind(Streams.layerMemory(), Streamsy.group(Input)),
  );
  return Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("CREATE TABLE effects (value INTEGER NOT NULL)");
      const store = yield* CheckpointStore;
      const failure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "before checkpoint",
      });
      const result = yield* Projection.run(sqlProcess).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(failure),
        }),
        Effect.result,
      );
      expect(result).toMatchObject({ _tag: "Failure", failure });
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([]);
      expect((yield* store.load(sqlProcess)).token).toBe("0");
      // Remove ambient SQL: the Commit still supplies the Transaction's exact client.
      yield* Projection.run(sqlProcess).pipe(
        Effect.updateContext((context: Context.Context<CheckpointStore | Transaction | Input>) =>
          Context.omit(SqlClient.SqlClient)(context),
        ),
      );
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }]);
      const folded = Projection.fold({
        id: "sql-fold",
        params: {},
        inputs: { input: Input },
        state: { schema: Schema.Number, initial: 10 },
        commit: Commit.transaction,
        fold: (previous, { item }) =>
          Effect.gen(function* () {
            const client = yield* SqlClient.SqlClient;
            yield* client`INSERT INTO effects (value) VALUES (${previous + item})`;
            return previous + item;
          }),
      }).at({});
      const failedFold = yield* Projection.run(folded).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(failure),
        }),
        Effect.result,
      );
      expect(failedFold).toMatchObject({ _tag: "Failure", failure });
      expect((yield* Projection.read(folded)).state).toBe(10);
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }]);
      yield* Projection.run(folded);
      expect((yield* Projection.read(folded)).state).toBe(11);
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }, { value: 11 }]);
    }).pipe(Effect.provide(host)),
  );
});

test("transaction checks all declared outputs before running the handler", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      let calls = 0;
      const base = Projection.emit({
        commit: Commit.producer,
        id: "owner",
        params: {},
        inputs: { input: Input },
        outputs: { output: Sink },
        emit: () =>
          Effect.sync(() => {
            calls += 1;
            return { output: [2] };
          }),
      }).at({});
      const tx = yield* Transaction;
      const result = yield* Projection.run({ ...base, commit: Commit.transaction }).pipe(
        Effect.provideService(Transaction, { ...tx, owner: {}, sql: Option.none() }),
        Effect.result,
      );
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { phase: "load", reason: "unsupported-composition" },
      });
      expect(calls).toBe(0);
      expect((yield* (yield* CheckpointStore).load(base)).token).toBe("0");
    }).pipe(Effect.provide(memory)),
  ));

test("transaction commits declared outputs and state together and rolls both back on conflict", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const sink = yield* Sink;
      yield* sink.create({});
      const base = Projection.mapAccumBatch({
        commit: Commit.producer,
        id: "transaction-outputs",
        params: {},
        inputs: { input: Input },
        outputs: { output: Sink },
        state: { schema: Schema.Number, initial: 0 },
        mapAccumBatch: (state, batch) =>
          Effect.succeed([
            state + batch.input.items.length,
            { output: batch.input.items },
          ] as const),
      }).at({});
      const projection = { ...base, commit: Commit.transaction };
      const store = yield* CheckpointStore;
      const failed = yield* Projection.run(projection).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () =>
            Effect.fail(
              new ProjectionFault({
                phase: "checkpoint",
                reason: "token-conflict",
                message: "lost compare-and-set",
              }),
            ),
        }),
        Effect.result,
      );
      expect(failed).toMatchObject({ _tag: "Failure", failure: { reason: "token-conflict" } });
      expect((yield* Projection.read(projection)).state).toBe(0);
      expect((yield* Stream.runCollect(sink.read({}))).flatMap((batch) => batch.items)).toEqual([]);
      yield* Projection.run(projection);
      expect((yield* Projection.read(projection)).state).toBe(1);
      expect((yield* Stream.runCollect(sink.read({}))).flatMap((batch) => batch.items)).toEqual([
        1,
      ]);
    }).pipe(Effect.provide(memory)),
  ));
