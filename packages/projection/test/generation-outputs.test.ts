import { layer } from "@effect/vitest";
import { expect, test } from "vitest";
import { Effect, Fiber, Option, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  StateTable,
} from "@streamsy/projection";
import { bindAtGeneration } from "../src/declaration.ts";
import { withFamilies } from "./support/families.ts";
import {
  Accepted,
  Facts,
  Numbers,
  Rows,
  accepted,
  declaration,
  failedHandler,
  numbers,
  output,
  sums,
  table,
} from "./support/generation-outputs.ts";
import { layerSqlite } from "./support/sqlite.ts";

const host = withFamilies(layerSqlite(), Facts, Numbers, Rows, Accepted);

const first = { id: "a", n: 3 };

const second = { id: "b", n: 5 };

const seed = (workspace: string) =>
  Effect.gen(function* () {
    const facts = yield* Facts;

    yield* facts.create({ workspace });

    return yield* facts.append({ workspace }, [first, second]);
  });

for (const commit of [Commit.transaction, Commit.producer]) {
  layer(host)((it) => {
    it.effect(`${commit.name}: JSON generations replay into separate streams`, () =>
      Effect.gen(function* () {
        const params = { workspace: "json" };
        yield* seed(params.workspace);
        const draft = numbers(commit, 2);
        const one = numbers(commit, 1).at(params);
        const two = draft.at(params);
        const wire = (yield* Numbers).wire;

        expect(one.streams.numbers?.id).toBe("g1/numbers/json/g/1");
        expect(two.streams.numbers?.id).toBe("g1/numbers/json/g/2");
        expect(bindAtGeneration(draft, params, 1)?.streams.numbers?.id).toBe(
          one.streams.numbers?.id,
        );
        expect(bindAtGeneration(draft, params, 1)?.accepted?.id).toBe(one.accepted?.id);

        yield* Projection.run(one);
        const oldId = Numbers.at({ ...params, rev: 1 }).id;
        const before = yield* wire.read(oldId, { offset: ZERO_OFFSET });
        yield* Projection.run(two);
        expect((yield* Projection.run(two)).units).toBe(0);
        expect((yield* (yield* Numbers).collect({ ...params, rev: 2 })).items).toEqual([3, 5]);
        expect(yield* wire.read(oldId, { offset: ZERO_OFFSET })).toEqual(before);
      }),
    );

    it.effect(`${commit.name}: a State rebuild drops a key from the new stream only`, () =>
      Effect.gen(function* () {
        const params = { workspace: "state" };
        yield* seed(params.workspace);
        yield* StateTable.migrate(yield* SqlClient.SqlClient);

        const changed = (generation: number) =>
          Projection.emit({
            ...declaration,
            id: "g1-state",
            generation,
            outputs: { rows: Projection.perGeneration(Rows, "rev") },
            commit,
            emit: ({ item }) => ({
              rows:
                generation === 2 && item.id === "a"
                  ? []
                  : [DurableState.upsert("sum", { id: item.id, count: item.n })],
            }),
          }).at(params);

        const old = { ...params, rev: 1 };
        const next = { ...params, rev: 2 };
        const rows = yield* Rows;
        const oldId = Rows.at(old).id;

        yield* Projection.run(changed(1));
        const before = yield* rows.wire.read(oldId, { offset: ZERO_OFFSET });
        yield* Projection.run(changed(2));
        yield* Projection.run(table.at(old));
        yield* Projection.run(table.at(next));
        expect((yield* table.at(old).list("sum")).rows).toEqual([
          { key: "a", value: { id: "a", count: 3 } },
          { key: "b", value: { id: "b", count: 5 } },
        ]);
        expect((yield* table.at(next).list("sum")).rows).toEqual([
          { key: "b", value: { id: "b", count: 5 } },
        ]);
        expect(yield* rows.wire.read(oldId, { offset: ZERO_OFFSET })).toEqual(before);
      }),
    );

    it.effect(`${commit.name}: accepted rows and waits are generation-specific`, () =>
      Effect.gen(function* () {
        const params = { workspace: "accepted" };
        const facts = yield* Facts;
        yield* facts.create(params);
        const firstWrite = yield* facts.append(params, [first]);
        const one = numbers(commit, 1).at(params);
        const two = numbers(commit, 2).at(params);
        const old = { ...params, epoch: 1 };
        const next = { ...params, epoch: 2 };
        const stream = Facts.at(params).id;
        const wake = yield* Accepted;

        yield* Projection.run(one);
        yield* Projection.createOutputs(two);

        const waiting = yield* Streamsy.awaitWake(Accepted, next, {
          stream,
          offset: firstWrite.offset,
        }).pipe(Effect.forkChild({ startImmediately: true }));

        expect(waiting.pollUnsafe()).toBeUndefined();
        expect((yield* wake.collect(next)).items).toEqual([]);

        const secondWrite = yield* facts.append(params, [second]);
        yield* Projection.run(two);
        yield* Fiber.join(waiting);
        expect((yield* wake.collect(old)).items).toMatchObject([
          { value: { stream, offset: firstWrite.offset } },
        ]);
        expect((yield* wake.collect(next)).items).toMatchObject([
          { value: { stream, offset: secondWrite.offset } },
        ]);
      }),
    );

    it.effect(`${commit.name}: rejects a replay into an occupied output unless shared`, () =>
      Effect.gen(function* () {
        const params = { workspace: "occupied" };
        yield* seed(params.workspace);

        const build = (id: string, shared = false) =>
          Projection.emit({
            ...declaration,
            id,
            outputs: { numbers: Projection.perGeneration(Numbers, "rev", { shared }) },
            commit,
            emit: ({ item }) => ({ numbers: [item.n] }),
          }).at(params);

        const bound = build("g1-occupied");
        const address = { ...params, rev: 1 };
        const target = yield* Numbers;
        const phase = commit === Commit.transaction ? "process" : "pin";

        yield* target.create(address);
        yield* Projection.run(bound);
        yield* Projection.forget(bound);
        expect(yield* Projection.follow(bound).pipe(Stream.runDrain, Effect.flip)).toMatchObject({
          phase,
          reason: "invalid-output",
          retryable: false,
        });
        expect((yield* commit.load(bound)).record).toEqual(Option.none());
        expect((yield* target.collect(address)).items).toEqual([3, 5]);
        yield* Projection.run(build("g1-shared", true));
        expect((yield* target.collect(address)).items).toEqual([3, 5, 3, 5]);
      }),
    );

    it.effect(`${commit.name}: replay after a gone input needs a new generation`, () =>
      Effect.gen(function* () {
        const params = { workspace: "recreated" };
        const facts = yield* Facts;
        const target = yield* Numbers;
        const wake = yield* Accepted;
        const bound = numbers(commit, 1).at(params);
        const oldOutput = { ...params, rev: 1 };
        const oldAccepted = { ...params, epoch: 1 };

        yield* facts.create(params);
        yield* facts.append(params, [first]);
        yield* Projection.run(bound);

        const acceptedBefore = (yield* wake.collect(oldAccepted)).items;
        expect(acceptedBefore).toHaveLength(1);
        expect((yield* target.collect(oldOutput)).items).toEqual([3]);
        expect(Option.isSome((yield* commit.load(bound)).record)).toBe(true);

        yield* facts.remove(params);
        expect(yield* Projection.run(bound)).toMatchObject({
          status: "source-closed",
          units: 1,
          items: 0,
        });
        expect((yield* commit.load(bound)).record).toEqual(Option.none());

        yield* facts.create(params);
        const appended = yield* facts.append(params, [second]);

        expect(yield* Projection.run(bound).pipe(Effect.flip)).toMatchObject({
          phase: commit === Commit.transaction ? "process" : "pin",
          reason: "invalid-output",
          retryable: false,
        });
        expect((yield* commit.load(bound)).record).toEqual(Option.none());

        expect((yield* Projection.run(numbers(commit, 2).at(params))).items).toBe(1);
        expect((yield* target.collect({ ...params, rev: 2 })).items).toEqual([5]);
        expect((yield* wake.collect({ ...params, epoch: 2 })).items).toMatchObject([
          { value: { stream: Facts.at(params).id, offset: appended.offset } },
        ]);
        expect((yield* target.collect(oldOutput)).items).toEqual([3]);
        expect((yield* wake.collect(oldAccepted)).items).toEqual(acceptedBefore);
      }),
    );

    it.effect(`${commit.name}: empty input and first unit without output items`, () =>
      Effect.gen(function* () {
        const params = { workspace: "empty" };
        const facts = yield* Facts;
        const target = yield* Numbers;
        const wake = yield* Accepted;

        const bound = Projection.emit({
          ...declaration,
          id: "g1-empty",
          outputs: { numbers: output, legacy: Numbers.at({ ...params, rev: 9 }) },
          accepted,
          commit,
          emit: () => ({}),
        }).at(params);

        const address = { ...params, rev: 1 };
        const wakeParams = { ...params, epoch: 1 };
        yield* facts.create(params);
        expect((yield* Projection.run(bound)).units).toBe(0);
        expect((yield* commit.load(bound)).record).toEqual(Option.none());
        expect(yield* target.head(address).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });
        expect(yield* wake.head(wakeParams).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });

        yield* facts.append(params, [first]);
        yield* Projection.run(bound);

        if (commit === Commit.producer)
          expect(yield* target.head(address).pipe(Effect.flip)).toMatchObject({
            _tag: "StreamNotFound",
          });
        else expect((yield* target.head(address)).nextOffset).toBe(ZERO_OFFSET);

        yield* Projection.createOutputs(bound);
        yield* Projection.createOutputs(bound);
        expect((yield* target.head(address)).nextOffset).toBe(ZERO_OFFSET);
        expect((yield* wake.collect(wakeParams)).items).toHaveLength(1);
        expect(yield* target.head({ ...params, rev: 9 }).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });
      }),
    );

    it.effect(`${commit.name}: explicitly provision an empty binding`, () =>
      Effect.gen(function* () {
        const params = { workspace: "provisioned" };
        const bound = numbers(commit, 2).at(params);
        yield* Projection.createOutputs(bound);
        yield* Projection.createOutputs(bound);
        expect((yield* (yield* Numbers).head({ ...params, rev: 2 })).nextOffset).toBe(ZERO_OFFSET);
        expect((yield* (yield* Accepted).head({ ...params, epoch: 2 })).nextOffset).toBe(
          ZERO_OFFSET,
        );
        expect((yield* commit.load(bound)).record).toEqual(Option.none());

        yield* seed(params.workspace);
        yield* Projection.run(bound);
        expect((yield* (yield* Numbers).collect({ ...params, rev: 2 })).items).toEqual([3, 5]);
      }),
    );
  });
}

layer(host)((it) => {
  it.effect(
    "transaction: FoldByKey folds each generation from empty and does not recreate a removed output",
    () =>
      Effect.gen(function* () {
        const params = { workspace: "fold" };
        const facts = yield* Facts;
        yield* facts.create(params);
        yield* facts.append(params, [first]);
        yield* StateTable.migrate(yield* SqlClient.SqlClient);
        const one = sums(1).at(params);
        const two = sums(2).at(params);
        const old = { ...params, rev: 1 };
        const next = { ...params, rev: 2 };
        const rows = yield* Rows;
        const oldId = Rows.at(old).id;

        yield* Projection.run(one);
        yield* facts.append(params, [first]);
        yield* Projection.run(one);
        const before = yield* rows.wire.read(oldId, { offset: ZERO_OFFSET });
        yield* Projection.run(two);
        yield* Projection.run(table.at(old));
        yield* Projection.run(table.at(next));
        expect((yield* table.at(old).get("sum", "a")).value).toEqual({ id: "a", count: 6 });
        expect((yield* table.at(next).get("sum", "a")).value).toEqual({ id: "a", count: 6 });
        expect(yield* rows.wire.read(oldId, { offset: ZERO_OFFSET })).toEqual(before);

        yield* rows.remove(next);
        yield* facts.append(params, [second]);
        expect(yield* Projection.run(two).pipe(Effect.flip)).toMatchObject({
          phase: "process",
          reason: "invalid-output",
        });
        expect(yield* rows.head(next).pipe(Effect.flip)).toMatchObject({ _tag: "StreamNotFound" });
      }),
  );

  it.effect("transaction: handler failure rolls back output creation and the record", () =>
    Effect.gen(function* () {
      const params = { workspace: "rollback" };
      yield* seed(params.workspace);
      const transaction = Commit.transaction;
      const bound = numbers(transaction, 2, true).at(params);
      const address = { ...params, rev: 2 };
      const wakeParams = { ...params, epoch: 2 };
      const target = yield* Numbers;
      const wake = yield* Accepted;
      let inside = false;

      const process = Projection.process({
        ...declaration,
        id: bound.id,
        generation: bound.generation,
        commit: transaction,
        outputs: { numbers: output },
        accepted,
        process: () =>
          Effect.gen(function* () {
            expect((yield* target.head(address)).nextOffset).toBe(ZERO_OFFSET);
            expect((yield* wake.head(wakeParams)).nextOffset).toBe(ZERO_OFFSET);
            inside = true;

            return yield* Effect.fail(failedHandler);
          }),
      }).at(params);

      expect(yield* Projection.run(process).pipe(Effect.flip)).toBe(failedHandler);
      expect(inside).toBe(true);
      expect(yield* target.head(address).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
      expect(yield* wake.head(wakeParams).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
      expect((yield* transaction.load(bound)).record).toEqual(Option.none());
      yield* Projection.run(numbers(transaction, 2).at(params));
      expect((yield* target.collect(address)).items).toEqual([3, 5]);
    }),
  );

  it.effect("producer: a pinned retry can finish after the generation outputs have data", () =>
    Effect.gen(function* () {
      const params = { workspace: "pinned" };
      yield* seed(params.workspace);
      const bound = numbers(Commit.producer, 2).at(params);
      const store = yield* CheckpointStore;
      const target = yield* Numbers;
      const wake = yield* Accepted;
      const address = { ...params, rev: 2 };

      const result = yield* Projection.run(bound).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (key, token, next) =>
            next.pending === undefined
              ? Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "failed settle",
                  }),
                )
              : store.compareAndSet(key, token, next),
        }),
        Effect.result,
      );

      expect(result._tag).toBe("Failure");
      expect((yield* target.collect(address)).items).toEqual([3, 5]);
      yield* Projection.run(bound);
      expect((yield* target.collect(address)).items).toEqual([3, 5]);
      expect((yield* wake.collect({ ...params, epoch: 2 })).items).toHaveLength(1);
    }),
  );

  it.effect("producer: a failed pin leaves an empty stream the next run can use", () =>
    Effect.gen(function* () {
      const params = { workspace: "pin-failed" };
      yield* seed(params.workspace);
      const bound = numbers(Commit.producer, 2).at(params);
      const store = yield* CheckpointStore;
      const target = yield* Numbers;
      const address = { ...params, rev: 2 };

      const failedPin = new ProjectionFault({
        phase: "pin",
        reason: "storage-failure",
        message: "failed pin",
      });

      expect(
        yield* Projection.run(bound).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: () => Effect.fail(failedPin),
          }),
          Effect.flip,
        ),
      ).toBe(failedPin);
      expect((yield* store.load(bound)).record).toEqual(Option.none());
      expect((yield* target.head(address)).nextOffset).toBe(ZERO_OFFSET);
      yield* Projection.run(bound);
      expect((yield* target.collect(address)).items).toEqual([3, 5]);
    }),
  );

  it.effect("producer: checks the first write even after an earlier unit emitted nothing", () =>
    Effect.gen(function* () {
      const params = { workspace: "later-output" };
      const facts = yield* Facts;
      const target = yield* Numbers;
      const address = { ...params, rev: 1 };

      const bound = Projection.emit({
        ...declaration,
        id: "g1-later-output",
        outputs: { numbers: output },
        commit: Commit.producer,
        emit: ({ item }) => ({ numbers: item.id === "a" ? [] : [item.n] }),
      }).at(params);

      yield* facts.create(params);
      yield* facts.append(params, [first]);
      yield* Projection.run(bound);
      expect(Option.isSome((yield* Commit.producer.load(bound)).record)).toBe(true);

      yield* target.create(address);
      yield* target.append(address, [100]);
      yield* facts.append(params, [second]);
      expect(yield* Projection.run(bound).pipe(Effect.flip)).toMatchObject({
        phase: "pin",
        reason: "invalid-output",
        retryable: false,
      });
      expect((yield* target.collect(address)).items).toEqual([100]);
    }),
  );
});

class Legacy extends Streamsy.json<Legacy>()("G1/Legacy", "g1/legacy/:workspace/:generation", {
  params: { workspace: Schema.String, generation: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

layer(withFamilies(layerSqlite(), Facts, Legacy, Numbers))((it) => {
  it.effect("legacy bindings keep their route params and transaction creation behavior", () =>
    Effect.gen(function* () {
      const params = { workspace: "legacy", generation: 44 };
      yield* seed(params.workspace);

      const build = (generation: number) =>
        Projection.emit({
          ...declaration,
          id: "g1-legacy",
          generation,
          params: { ...declaration.params, generation: Schema.FiniteFromString },
          outputs: { legacy: Legacy, fixed: Numbers.at({ workspace: "static", rev: 7 }) },
          commit: Commit.transaction,
          emit: ({ item }) => ({ legacy: [item.n] }),
        }).at(params);

      const one = build(1);
      const two = build(2);

      for (const bound of [one, two]) {
        expect(bound.streams.legacy?.id).toBe("g1/legacy/legacy/44");
        expect(bound.streams.fixed?.id).toBe("g1/numbers/static/g/7");
      }

      yield* Projection.createOutputs(one);
      expect(yield* Projection.run(two).pipe(Effect.flip)).toMatchObject({
        phase: "process",
        reason: "invalid-output",
      });
      expect(yield* (yield* Legacy).head(params).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
    }),
  );
});

test("generation bindings preserve output collision checks and propagate codec failures", () => {
  const params = { workspace: "collisions" };

  const config = {
    ...declaration,
    id: "g1-collisions",
    generation: 2,
    commit: Commit.transaction,
    emit: () => ({}),
  };

  expect(() =>
    Projection.emit({
      ...config,
      outputs: { wrapped: output, fixed: Numbers.at({ ...params, rev: 2 }) },
    }).at(params),
  ).toThrow("Two outputs may not name the same stream");

  expect(() =>
    Projection.emit({
      ...config,
      outputs: { output: accepted },
      accepted,
    }).at(params),
  ).toThrow("Two outputs may not name the same stream");

  expect(() =>
    Projection.emit({
      ...config,
      inputs: { fixed: Numbers.at({ ...params, rev: 2 }) },
      outputs: { wrapped: output },
    }).at(params),
  ).toThrow("An output may not name an input stream");

  expect(() => numbers(Commit.transaction, Infinity).at(params)).toThrow(RangeError);
});

test("runtime callers cannot bind a parameter from two sources", () => {
  const config = {
    ...declaration,
    id: "g1-overlap",
    params: { ...declaration.params, rev: Schema.FiniteFromString, epoch: Schema.FiniteFromString },
    commit: Commit.transaction,
    emit: () => ({}),
  };

  expect(() =>
    Projection.emit({
      ...config,
      // @ts-expect-error runtime callers are subject to the same collision rule
      outputs: { wrapped: output },
    }),
  ).toThrow("Generation-bound parameter rev must not be a projection parameter");

  expect(() =>
    Projection.emit({
      ...config,
      outputs: {},
      // @ts-expect-error the overlap check applies to the accepted route too
      accepted,
    }),
  ).toThrow("Generation-bound parameter epoch must not be a projection parameter");
});
