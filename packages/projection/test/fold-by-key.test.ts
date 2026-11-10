import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Cause, Effect, Exit, Layer, Option, Schema, Schedule } from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/sql";
import { DurableState, StreamAddress, StorageFault, Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { ProjectionMarks } from "@streamsy/core/internal";
import {
  CheckpointStore,
  Commit,
  FoldByKey,
  FoldByKeyError,
  Projection,
  ProjectionFault,
  StateTable,
  Transaction,
  type ProjectionKey,
} from "@streamsy/projection";
import { Facts, Workspace, Table, fold, sums, type Row, type Fact } from "./support/fold-by-key.ts";
import { layerSqlite } from "./support/sqlite.ts";

const host = (readLimit = 1000) => {
  const local = layerSqlite(readLimit);

  const streams = Protocol.bind(local, Streamsy.group(Facts, Workspace)).pipe(
    Layer.provideMerge(local),
  );

  return Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, StateTable.migrate)).pipe(
    Layer.provideMerge(streams),
  );
};

const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, check: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1000; turn += 1) {
      const value = yield* read;

      if (check(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

const params = { workspace: "main", source: "events" };

const transaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.flatMap(Transaction, (tx) => tx.withTransaction(body));

const sync = (workspace = "main") => transaction(Table.at({ workspace }).sync());

const rows = (workspace = "main") => Table.at({ workspace }).list("sum");

const seed = (records: Array.NonEmptyReadonlyArray<Fact>, values = params) =>
  Effect.gen(function* () {
    const facts = yield* Facts;
    yield* facts.create(values);
    yield* facts.append(values, records);
  });

const failure = new FoldByKeyError({ message: "injected failure" });

const clean = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM streamsy_state_rows`;
  yield* sql`DELETE FROM streamsy_state_heads`;
  yield* sql`DELETE FROM streamsy_messages`;
  yield* sql`DELETE FROM streamsy_producers`;
  yield* sql`DELETE FROM streamsy_streams`;
  yield* sql`DELETE FROM streamsy_owner_records`;
  yield* sql`DELETE FROM streamsy_projection_pending`;
});

const database = (id: string, binding: ProjectionKey) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    return {
      stream: yield* sql`SELECT * FROM streamsy_streams WHERE stream_id = ${id}`,
      messages:
        yield* sql`SELECT * FROM streamsy_messages WHERE stream_id = ${id} ORDER BY msg_offset`,
      rows: yield* sql`SELECT * FROM streamsy_state_rows WHERE stream_id = ${id} ORDER BY collection, row_key`,
      heads: yield* sql`SELECT * FROM streamsy_state_heads WHERE stream_id = ${id}`,
      checkpoint: yield* (yield* CheckpointStore).load(binding),
    };
  });

layer(host())((it) => {
  it.effect(
    "groups in first-seen order, folds records in order, skips undefined keys and appends once per key",
    () =>
      Effect.gen(function* () {
        yield* clean;
        const order: string[] = [];

        const declaration = Projection.process({
          id: "fold-order",
          params: { workspace: Schema.String, source: Schema.String },
          inputs: { facts: Facts },
          outputs: { state: Workspace },
          commit: Commit.transaction,
          process: FoldByKey.process(Table, {
            output: "state",
            collection: "sum",
            records: (item) => [item, { ...item, n: item.n * 10 }],
            key: (record) => (record.id === "skip" ? undefined : record.id),
            fold: (previous: Row | undefined, record) => {
              order.push(`${record.id}:${record.n}`);

              return fold(previous, record);
            },
          }),
        }).at(params);

        yield* seed([
          { id: "b", n: 1 },
          { id: "a", n: 2 },
          { id: "b", n: 3 },
          { id: "skip", n: 100 },
        ]);
        yield* Projection.run(declaration);
        expect(order).toEqual(["b:1", "b:10", "b:3", "b:30", "a:2", "a:20"]);
        const events = (yield* (yield* Workspace).collect(params)).items;
        expect(events).toHaveLength(2);
        expect(
          Array.map(events, (event) => (StreamAddress.isChange(event) ? event.key : "control")),
        ).toEqual(["b", "a"]);
        // S2: the fold created a head at zero, but did not apply its own append.
        expect((yield* rows()).rows).toEqual([]);
        yield* sync();
        expect((yield* rows()).rows).toEqual([
          { key: "a", value: { id: "a", count: 22 } },
          { key: "b", value: { id: "b", count: 44 } },
        ]);
      }),
  );

  it.effect("loads previous rows beyond 50 keys and drops unchanged rows", () =>
    Effect.gen(function* () {
      yield* clean;
      const records = Array.makeBy(113, (n) => ({ id: String(n).padStart(3, "0"), n: 1 }));
      yield* seed(records);
      const binding = sums().at(params);
      yield* Projection.run(binding);
      expect((yield* rows()).rows).toEqual([]);
      yield* (yield* Facts).append(params, records);
      yield* Projection.run(binding);
      // The second unit's sync has applied the first append, and getMany read all 113 keys.
      expect(Array.map((yield* rows()).rows, (row) => row.value.count)).toEqual(
        Array.makeBy(113, () => 1),
      );
      yield* sync();
      expect(Array.map((yield* rows()).rows, (row) => row.value.count)).toEqual(
        Array.makeBy(113, () => 2),
      );
      const stream = yield* Workspace;
      const before = yield* stream.head(params);
      yield* (yield* Facts).append(params, [{ id: "000", n: 0 }]);
      yield* Projection.run(binding);
      expect((yield* stream.head(params)).nextOffset).toBe(before.nextOffset);
    }),
  );

  it.effect(
    "remove deletes a row and later records restart from undefined; absent deletes append nothing",
    () =>
      Effect.gen(function* () {
        yield* clean;

        const binding = Projection.process({
          id: "removals",
          params: { workspace: Schema.String, source: Schema.String },
          inputs: { facts: Facts },
          outputs: { state: Workspace },
          commit: Commit.transaction,
          process: FoldByKey.process(Table, {
            output: "state",
            collection: "sum",
            key: (record) => record.id,
            fold: (previous: Row | undefined, record) =>
              record.n < 0 ? FoldByKey.remove : fold(previous, record),
          }),
        }).at(params);

        yield* seed([
          { id: "a", n: 5 },
          { id: "b", n: 8 },
        ]);
        yield* Projection.run(binding);
        yield* (yield* Facts).append(params, [
          { id: "a", n: -1 },
          { id: "b", n: -1 },
          { id: "b", n: 3 },
          { id: "missing", n: -1 },
        ]);
        yield* Projection.run(binding);
        const events = (yield* (yield* Workspace).collect(params)).items;
        expect(events).toHaveLength(4);
        expect(events[2]).toMatchObject({ key: "a", headers: { operation: "delete" } });
        yield* Projection.run(Table.at(params));
        expect((yield* rows()).rows).toEqual([{ key: "b", value: { id: "b", count: 3 } }]);
      }),
  );

  for (const step of ["records", "key", "fold", "die"] as const) {
    it.effect(`a ${step} callback defect stays a defect, as in every handler kind`, () =>
      Effect.gen(function* () {
        yield* clean;
        const cause = new Error(`${step} exploded`);

        const binding = Projection.process({
          id: `throw-${step}`,
          params: { workspace: Schema.String, source: Schema.String },
          inputs: { facts: Facts },
          outputs: { state: Workspace },
          commit: Commit.transaction,
          process: FoldByKey.process(Table, {
            output: "state",
            collection: "sum",
            records: (item) => {
              if (step === "records") throw cause;

              return [item];
            },
            key: (record) => {
              if (step === "key") throw cause;

              return record.id;
            },
            fold: (previous: Row | undefined, record) => {
              if (step === "fold") throw cause;

              return step === "die" ? Effect.die(cause) : fold(previous, record);
            },
          }),
        }).at(params);

        yield* seed([{ id: "a", n: 1 }]);
        const exit = yield* Projection.run(binding).pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);

        if (Exit.isFailure(exit)) {
          expect(Option.isNone(Cause.findErrorOption(exit.cause))).toBe(true);
          expect(Cause.squash(exit.cause)).toBe(cause);
        }
      }),
    );
  }

  it.effect("typed fold failures pass through unchanged; interruption stays interruption", () =>
    Effect.gen(function* () {
      yield* clean;

      const binding = Projection.process({
        id: "typed-failure",
        params: { workspace: Schema.String, source: Schema.String },
        inputs: { facts: Facts },
        outputs: { state: Workspace },
        commit: Commit.transaction,
        process: FoldByKey.process(Table, {
          output: "state",
          collection: "sum",
          key: (record) => record.id,
          fold: () => Effect.fail(failure),
        }),
      }).at(params);

      yield* seed([{ id: "a", n: 1 }]);
      expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure,
      });

      const interrupted = Projection.process({
        id: "interrupted",
        params: { workspace: Schema.String, source: Schema.String },
        inputs: { facts: Facts },
        outputs: { state: Workspace },
        commit: Commit.transaction,
        process: FoldByKey.process(Table, {
          output: "state",
          collection: "sum",
          key: (record) => record.id,
          fold: () => Effect.interrupt,
        }),
      }).at(params);

      const exit = yield* Projection.run(interrupted).pipe(Effect.exit);
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    }),
  );

  it.effect("a key mismatch fails with both keys in FoldByKeyError", () =>
    Effect.gen(function* () {
      yield* clean;
      yield* seed([{ id: "loaded", n: 1 }]);

      const binding = Projection.process({
        id: "mismatch",
        params: { workspace: Schema.String, source: Schema.String },
        inputs: { facts: Facts },
        outputs: { state: Workspace },
        commit: Commit.transaction,
        process: FoldByKey.process(Table, {
          output: "state",
          collection: "sum",
          key: (record) => record.id,
          fold: () => ({ id: "folded", count: 1 }),
        }),
      }).at(params);

      expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "FoldByKeyError", message: expect.stringMatching(/loaded.*folded/) },
      });
    }),
  );

  it.effect("a stored row that the schema rejects fails with an invalid-record fault", () =>
    Effect.gen(function* () {
      yield* clean;
      const stream = yield* Workspace;
      yield* stream.create(params);
      yield* stream.wire.append(Workspace.at(params).id, {
        contentType: "application/json",
        data: new TextEncoder().encode(
          '{"type":"sum","key":"a","value":{"id":"a","count":"many"},"headers":{"operation":"upsert"}}',
        ),
      });
      yield* seed([{ id: "a", n: 1 }]);

      expect(yield* Projection.run(sums("bad-row").at(params)).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ProjectionFault", phase: "process", reason: "invalid-record" },
      });
    }),
  );

  it.effect(
    "two attached projections write different collections of one attached State stream",
    () =>
      Effect.gen(function* () {
        yield* clean;

        const statuses = Projection.process({
          id: "fold-status",
          params: { workspace: Schema.String, source: Schema.String },
          inputs: { facts: Facts },
          outputs: { state: Workspace },
          commit: Commit.transaction,
          process: FoldByKey.process(Table, {
            output: "state",
            collection: "status",
            key: (record) => record.id,
            fold: (_previous, record) => ({ id: record.id, label: String(record.n) }),
          }),
        });

        yield* Layer.build(
          Layer.mergeAll(
            Projection.attach(Table, { durable: true }),
            Projection.attach(sums(), { durable: true }),
            Projection.attach(statuses, { durable: true }),
          ),
        );
        yield* seed([
          { id: "a", n: 1 },
          { id: "b", n: 2 },
          { id: "a", n: 3 },
        ]);
        expect((yield* eventually(rows(), (value) => value.rows.length === 2)).rows).toHaveLength(
          2,
        );
        expect(
          (yield* eventually(Table.at(params).list("status"), (value) => value.rows.length === 2))
            .rows,
        ).toHaveLength(2);
        const stream = yield* Workspace;
        const tail = (yield* stream.head(params)).nextOffset;
        expect(yield* eventually(Table.at(params).offset(), (offset) => offset === tail)).toBe(
          tail,
        );

        const expected = DurableState.materialize(
          DurableState.materialized(Workspace.at(params)),
          (yield* stream.collect(params)).items,
        );

        for (const collection of ["sum", "status"] as const)
          expect(
            new Map(
              Array.map((yield* Table.at(params).list(collection)).rows, ({ key, value }) => [
                key,
                value,
              ]),
            ),
          ).toEqual(expected.rows[collection]);
      }),
  );
});

for (const step of [
  "create",
  "sync",
  "records",
  "key",
  "fold",
  "encode",
  "append",
  "checkpoint",
] as const) {
  layer(host())((it) => {
    it.effect(
      `a failure at ${step} rolls back stream, rows, head and checkpoint; retry counts once`,
      () =>
        Effect.gen(function* () {
          const workspace = `atomic-${step}`;
          const values = { workspace, source: "source" };
          const address = Workspace.at(values);
          const stream = yield* Workspace;
          const sql = yield* SqlClient.SqlClient;
          const store = yield* CheckpointStore;
          let broken = true;

          const storageFailure = new StorageFault({
            operation: "fold-test",
            message: "injected wire failure",
            retryable: true,
          });

          const checkpointFailure = new ProjectionFault({
            phase: "checkpoint",
            reason: "storage-failure",
            message: "injected save failure",
          });

          const binding = Projection.process({
            id: `atomic-${step}`,
            params: { workspace: Schema.String, source: Schema.String },
            inputs: { facts: Facts },
            outputs: { state: Workspace },
            commit: Commit.transaction,
            process: FoldByKey.process(Table, {
              output: "state",
              collection: "sum",
              records: (item) => {
                if (broken && step === "records") throw failure;

                return [item];
              },
              key: (record) => {
                if (broken && step === "key") throw failure;

                return record.id;
              },
              fold: (previous: Row | undefined, record) => {
                if (broken && step === "fold") return Effect.fail(failure);

                return broken && step === "encode"
                  ? { id: record.id, count: Number.NaN }
                  : fold(previous, record);
              },
            }),
          }).at(values);

          if (step !== "create") {
            yield* stream.create(values);
            yield* stream.append(
              values,
              DurableState.changes(address, {}, [
                DurableState.upsert("sum", { id: "a", count: 8 }),
              ]),
            );
            yield* sync(workspace);
            yield* stream.append(
              values,
              DurableState.changes(address, {}, [
                DurableState.upsert("sum", { id: "a", count: 10 }),
              ]),
            );
          }

          yield* seed([{ id: "a", n: 5 }], values);
          const before = yield* database(address.id, binding);

          if (step === "sync")
            yield* sql.unsafe(
              "CREATE TRIGGER fail_fold_sync BEFORE INSERT ON streamsy_state_heads BEGIN SELECT RAISE(ABORT, 'sync failure'); END",
            );

          const patched = {
            ...stream,
            wire: {
              ...stream.wire,
              create: (...args: Parameters<typeof stream.wire.create>) =>
                stream.wire
                  .create(...args)
                  .pipe(
                    Effect.tap(() =>
                      broken && step === "create" ? Effect.fail(storageFailure) : Effect.void,
                    ),
                  ),
              append: (...args: Parameters<typeof stream.wire.append>) =>
                stream.wire
                  .append(...args)
                  .pipe(
                    Effect.tap(() =>
                      broken && step === "append" ? Effect.fail(storageFailure) : Effect.void,
                    ),
                  ),
            },
          };

          const exit = yield* Projection.run(binding).pipe(
            Effect.provideService(Workspace, patched),
            Effect.provideService(CheckpointStore, {
              ...store,
              compareAndSet: (...args) =>
                broken && step === "checkpoint"
                  ? Effect.fail(checkpointFailure)
                  : store.compareAndSet(...args),
            }),
            Effect.exit,
          );

          expect(Exit.isFailure(exit)).toBe(true);

          // A wire, SQL or encoding failure of the handler is a ProjectionFault.
          const reason = new Map([
            ["create", "storage-failure"],
            ["sync", "storage-failure"],
            ["encode", "invalid-output"],
            ["append", "storage-failure"],
          ]).get(step);

          if (reason !== undefined && Exit.isFailure(exit))
            expect(Option.getOrThrow(Cause.findErrorOption(exit.cause))).toMatchObject({
              _tag: "ProjectionFault",
              phase: "process",
              reason,
            });
          expect(yield* database(address.id, binding)).toEqual(before);

          if (step === "sync") yield* sql.unsafe("DROP TRIGGER fail_fold_sync");
          broken = false;
          yield* Projection.run(binding);
          yield* sync(workspace);
          expect((yield* Table.at(values).get("sum", "a")).value?.count).toBe(
            step === "create" ? 5 : 15,
          );
          expect((yield* stream.collect(values)).items).toHaveLength(step === "create" ? 1 : 3);
        }),
    );
  });
}

for (const scenario of [
  "correct",
  "generation-before-reset",
  "old-runner-after-reset",
  "rename",
] as const) {
  layer(host())((it) => {
    it.effect(`fold fix and identity rule: ${scenario}`, () =>
      Effect.gen(function* () {
        const values = { workspace: scenario, source: "a" };
        const second = { ...values, source: "b" };
        const declaration = sums("operator-fold");
        yield* seed([{ id: "counter", n: 10 }], values);
        yield* seed([{ id: "counter", n: 20 }], second);
        yield* Projection.run(declaration.at(values));
        yield* Projection.run(declaration.at(second));
        yield* sync(values.workspace);
        expect((yield* Table.at(values).get("sum", "counter")).value?.count).toBe(30);

        if (scenario === "correct") {
          // No old runners remain. Reset before raising every writer's generation, then run every source.
          yield* StateTable.reset(Table.at(values));
          yield* Projection.run(sums("operator-fold", 2).at(values));
          yield* Projection.run(sums("operator-fold", 2).at(second));
          const fresh = { workspace: "fresh", source: "a" };
          yield* seed([{ id: "counter", n: 10 }], fresh);
          yield* seed([{ id: "counter", n: 20 }], { ...fresh, source: "b" });
          yield* Projection.run(sums("fresh-fold", 2).at(fresh));
          yield* Projection.run(sums("fresh-fold", 2).at({ ...fresh, source: "b" }));
          yield* sync("fresh");
          yield* sync(values.workspace);
          expect((yield* rows(values.workspace)).rows).toEqual((yield* rows("fresh")).rows);
          expect((yield* Table.at(values).get("sum", "counter")).value?.count).toBe(30);
        } else if (scenario === "old-runner-after-reset") {
          yield* StateTable.reset(Table.at(values));
          yield* (yield* Facts).append(values, [{ id: "counter", n: 4 }]);
          yield* Projection.run(declaration.at(values));
          yield* sync(values.workspace);
          // Only the post-checkpoint delta remains: 30 history + 4 should have been 34.
          expect((yield* Table.at(values).get("sum", "counter")).value?.count).toBe(4);
        } else {
          const replacement =
            scenario === "rename" ? sums("renamed-fold") : sums("operator-fold", 2);

          yield* Projection.run(replacement.at(values));
          yield* Projection.run(replacement.at(second));
          yield* sync(values.workspace);
          // A rename or a generation raised before reset folds all 30 over the kept 30.
          expect((yield* Table.at(values).get("sum", "counter")).value?.count).toBe(60);
        }
      }),
    );
  });
}

for (const durable of [true, false]) {
  layer(host())((it) => {
    it.effect(
      `a mismatched binding under ${durable ? "durable" : "memory"} attach retries while a neighbour runs`,
      () =>
        Effect.gen(function* () {
          let fixed = false;
          let attempts = 0;

          const declaration = Projection.process({
            id: "attached-mismatch",
            params: { workspace: Schema.String, source: Schema.String },
            inputs: { facts: Facts },
            outputs: { state: Workspace },
            commit: Commit.transaction,
            process: FoldByKey.process(Table, {
              output: "state",
              collection: "sum",
              key: (record) => record.id,
              fold: (previous: Row | undefined, record, unit) => {
                if (unit.params.source === "bad") {
                  attempts += 1;

                  if (!fixed) return { id: "mismatch", count: 1 };
                }

                return fold(previous, record);
              },
            }),
          });

          yield* Layer.build(
            Projection.attach(declaration, { durable, retry: Schedule.spaced("1 second") }),
          );
          yield* seed([{ id: "bad", n: 5 }], { workspace: "main", source: "bad" });
          yield* seed([{ id: "good", n: 7 }], { workspace: "main", source: "good" });

          const readGood = Effect.gen(function* () {
            const stream = yield* Workspace;

            return yield* stream.collect(params).pipe(
              Effect.map((result) => result.items),
              Effect.catchTag("StreamNotFound", () => Effect.succeed([])),
            );
          });

          expect(yield* eventually(readGood, (events) => events.length === 1)).toHaveLength(1);
          expect(attempts).toBeGreaterThan(0);

          if (durable) expect(yield* (yield* ProjectionMarks).deferred(declaration.id)).toBe(1);
          fixed = true;
          yield* TestClock.adjust("1 second");
          expect(yield* eventually(readGood, (events) => events.length === 2)).toHaveLength(2);
          expect(attempts).toBeGreaterThan(1);
          yield* sync();
          expect((yield* Table.at(params).get("sum", "bad")).value?.count).toBe(5);
        }),
    );
  });
}
