import { layer } from "@effect/vitest";
import { describe, expect, test } from "vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  StateTable,
} from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";

class Input extends Streamsy.json<Input>()("Accepted/Input", "accepted/input/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

class Accepted extends Streamsy.wake<Accepted>()("Accepted/Wake", "accepted/wake/:id", {
  params: { id: Schema.String },
}) {}

class Rows extends Streamsy.state<Rows>()("Accepted/Rows", "accepted/rows/:id", {
  params: { id: Schema.String },
  collections: { row: { schema: Schema.Struct({ id: Schema.String }), key: "id" } },
}) {}

const fixedParams = { id: "one" };

const definition = {
  id: "accepted-fold",
  params: { id: Schema.String },
  inputs: { input: Input },
  accepted: Accepted,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state: number, { item }: { item: number }) => state + item,
};

const folded = Projection.fold({ ...definition, commit: Commit.transaction });

const graph = layerSqlite(1);

const host = Protocol.bind(graph, Streamsy.group(Input, Accepted, Rows)).pipe(
  Layer.provideMerge(graph),
);

const changes = (params: { id: string }) =>
  Effect.gen(function* () {
    return (yield* (yield* Accepted).collect(params)).items;
  });

const seed = (params: { id: string }) =>
  Effect.gen(function* () {
    const input = yield* Input;
    yield* input.create(params);
    yield* (yield* Accepted).create(params);

    return yield* input.append(params, [1, 2]);
  });

const failure = new ProjectionFault({
  phase: "checkpoint",
  reason: "storage-failure",
  message: "failed settle",
});

layer(host)((it) => {
  it.effect("fold run commits one accepted row per unit and rolls back on failed settle", () =>
    Effect.gen(function* () {
      const params = { id: "case-1" };
      const tail = yield* seed(params);
      const store = yield* CheckpointStore;
      const bound = folded.at(params);

      const failed = yield* Projection.run(bound).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(failure),
        }),
        Effect.result,
      );

      expect(failed._tag).toBe("Failure");
      expect(yield* changes(params)).toEqual([]);
      expect((yield* Projection.read(bound)).state).toBe(0);
      yield* Projection.run(bound);
      const rows = yield* changes(params);
      expect(rows).toHaveLength(2);
      expect(rows[1]).toMatchObject({
        value: { stream: Input.at(params).id, offset: tail.offset },
      });
      yield* Projection.run(bound);
      expect(yield* changes(params)).toHaveLength(2);
    }),
  );
  it.effect("producer fold retries its pinned accepted output exactly once", () =>
    Effect.gen(function* () {
      const params = { id: "case-2" };
      yield* seed(params);
      const bound = Projection.fold({ ...definition, commit: Commit.producer }).at(params);
      const store = yield* CheckpointStore;

      const failed = yield* Projection.run(bound, { limit: 1 }).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (key, token, next) =>
            next.pending === undefined
              ? Effect.fail(failure)
              : store.compareAndSet(key, token, next),
        }),
        Effect.result,
      );

      expect(failed._tag).toBe("Failure");
      expect(yield* changes(params)).toHaveLength(1);
      yield* Projection.run(bound, { limit: 1 });
      expect(yield* changes(params)).toHaveLength(1);
      expect((yield* Projection.read(bound)).state).toBe(1);
    }),
  );
  it.effect("follow publishes accepted offsets", () =>
    Effect.gen(function* () {
      const params = { id: "case-3" };
      const tail = yield* seed(params);
      yield* Projection.follow(folded.at(params)).pipe(Stream.take(1), Stream.runDrain);
      yield* Streamsy.awaitWake(Accepted, params, {
        stream: Input.at(params).id,
        offset: tail.offset,
      });
    }),
  );
  it.effect("attach publishes accepted offsets with no runner-specific output path", () =>
    Effect.gen(function* () {
      const params = { id: "case-4" };
      yield* (yield* Accepted).create(params);
      yield* Layer.build(Projection.attach(folded));
      const input = yield* Input;
      yield* input.create(params);
      const tail = yield* input.append(params, [3]);
      yield* Streamsy.awaitWake(Accepted, params, {
        stream: Input.at(params).id,
        offset: tail.offset,
      }).pipe(Effect.timeout("2 seconds"));
    }),
  );
  it.effect("a transaction process emits accepted rows after its handler succeeds", () =>
    Effect.gen(function* () {
      const params = { id: "case-5" };
      const tail = yield* seed(params);
      let count = 0;

      const process = Projection.process({
        id: "accepted-process",
        params: { id: Schema.String },
        inputs: { input: Input },
        accepted: Accepted,
        commit: Commit.transaction,
        process: (batch) =>
          Effect.sync(() => {
            count += batch.input.items.length;
          }),
      });

      yield* Projection.run(process.at(params));
      expect(count).toBe(2);
      yield* Streamsy.awaitWake(Accepted, params, {
        stream: Input.at(params).id,
        offset: tail.offset,
      });
    }),
  );
  it.effect("StateTable writes accepted rows in the table transaction", () =>
    Effect.gen(function* () {
      const params = { id: "case-6" };
      const table = StateTable.define(Rows, { accepted: Accepted }).at(params);
      yield* StateTable.migrate(yield* SqlClient.SqlClient);
      const rows = yield* Rows;
      yield* rows.create(params);
      yield* (yield* Accepted).create(params);

      const tail = yield* rows.append(
        params,
        DurableState.changes(Rows.at(params), {}, [DurableState.upsert("row", { id: "a" })]),
      );

      yield* Projection.run(table);
      expect((yield* table.get("row", "a")).value).toEqual({ id: "a" });
      yield* Streamsy.awaitWake(Accepted, params, {
        stream: Rows.at(params).id,
        offset: tail.offset,
      });
    }),
  );
  it.effect("only moved inputs or a new close publish rows alongside batch outputs", () =>
    Effect.gen(function* () {
      const params = { id: "multi-input" };
      const a = { id: "a" };
      const b = { id: "b" };
      const input = yield* Input;
      yield* input.create(a);
      yield* input.create(b);
      yield* (yield* Accepted).create(params);

      const projection = Projection.mapAccumBatch({
        id: "accepted-batch",
        params: { id: Schema.String },
        inputs: { a: Input.at(a), b: Input.at(b) },
        accepted: Accepted,
        outputs: { rows: Rows },
        commit: Commit.transaction,
        state: { schema: Schema.Finite, initial: 0 },
        mapAccumBatch: (state, batch) =>
          [
            state + batch.a.items.length + batch.b.items.length,
            { rows: [DurableState.upsert("row", { id: "batch" })] },
          ] as const,
      }).at(params);

      yield* (yield* Rows).create(params);
      const tail = yield* input.append(a, [1]);
      yield* Projection.run(projection);
      expect(yield* changes(params)).toMatchObject([
        { value: { stream: Input.at(a).id, offset: tail.offset } },
      ]);
      expect((yield* (yield* Rows).collect(params)).items).toHaveLength(1);
      yield* input.append(a, [], { close: true });
      yield* Projection.run(projection);
      expect(yield* changes(params)).toHaveLength(2);
      const bTail = yield* input.append(b, [2]);
      yield* Projection.run(projection);
      const accepted = yield* changes(params);
      expect(accepted).toHaveLength(3);
      expect(accepted[1]).toMatchObject({ value: { stream: Input.at(a).id, closed: true } });
      expect(accepted[2]).toMatchObject({
        value: { stream: Input.at(b).id, offset: bTail.offset },
      });
    }),
  );
  it.effect("a failed transaction handler writes no accepted row", () =>
    Effect.gen(function* () {
      const params = { id: "failed-handler" };
      yield* seed(params);

      const projection = Projection.process({
        id: "failed-handler",
        params: { id: Schema.String },
        inputs: { input: Input },
        accepted: Accepted,
        commit: Commit.transaction,
        process: () => Effect.fail("handler failed"),
      }).at(params);

      expect((yield* Projection.run(projection).pipe(Effect.result))._tag).toBe("Failure");
      expect(yield* changes(params)).toEqual([]);
    }),
  );
});

describe("accepted declaration validation", () => {
  const message = "accepted requires Commit.transaction or Commit.producer";
  test("rejects the fold default, checkpoint, atLeastOnce, custom and unsupported compensation", () => {
    // @ts-expect-error JavaScript callers still get the runtime validation backstop
    expect(() => Projection.fold(definition)).toThrow(message);

    for (const commit of [
      Commit.checkpoint,
      Commit.atLeastOnce,
      Commit.make({ ...Commit.transaction }),
      Commit.withCompensation(Commit.checkpoint, () => Effect.void),
    ])
      expect(() =>
        Projection.fold({
          ...definition,
          // @ts-expect-error unsupported strategies do not all accept output results
          commit,
        }),
      ).toThrow(message);
  });
  test("supported compensation retains output support", () => {
    expect(() =>
      Projection.fold({
        ...definition,
        commit: Commit.withCompensation(Commit.transaction, () => Effect.void),
      }),
    ).not.toThrow();
  });
  test("rejects accepted as an input when the declaration is built", () => {
    expect(() =>
      Projection.process({
        id: "cycle",
        params: { id: Schema.String },
        inputs: { wake: Accepted },
        accepted: Accepted,
        commit: Commit.transaction,
        process: () => Effect.void,
      }),
    ).toThrow("An accepted stream may not name an input stream");
  });
  test("checks fixed input aliases against the resolved accepted address at bind time", () => {
    const projection = Projection.process({
      id: "fixed-cycle",
      params: { id: Schema.String },
      inputs: { wake: Accepted.at(fixedParams) },
      accepted: Accepted,
      commit: Commit.transaction,
      process: () => Effect.void,
    });

    expect(() => projection.at(fixedParams)).toThrow("An output may not name an input stream");
    expect(() => projection.at({ id: "other" })).not.toThrow();
  });
  test("rejects the reserved accepted output name at declaration build", () => {
    expect(() =>
      Projection.emit({
        id: "reserved-output",
        params: { id: Schema.String },
        inputs: { input: Input },
        outputs: { "@streamsy/accepted": Rows },
        accepted: Accepted,
        commit: Commit.transaction,
        emit: () => ({}),
      }),
    ).toThrow("Output name @streamsy/accepted is reserved for accepted");
  });
  test("rejects accepted params absent from the projection", () => {
    expect(() =>
      Projection.process({
        id: "bad-params",
        params: {},
        inputs: {},
        // @ts-expect-error accepted params must be projection params
        accepted: Accepted,
        commit: Commit.transaction,
        process: () => Effect.void,
      }),
    ).toThrow("accepted parameter id must be a projection parameter");
  });
});
