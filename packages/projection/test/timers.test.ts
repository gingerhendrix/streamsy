import { layer } from "@effect/vitest";
import { expect, test } from "vitest";
import { DateTime, Effect, Layer, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  type Unit,
} from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { layerSqlite } from "./support/sqlite.ts";
import { unitOf } from "../src/unit.ts";

class Input extends Streamsy.json<Input>()("timers/Input", "timers/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Accepted extends Streamsy.wake<Accepted>()("timers/Accepted", "timers/accepted", {
  params: {},
}) {}

const definition = { id: "timer", params: {}, inputs: { input: Input } };

const seed = Effect.gen(function* () {
  yield* TestClock.setTime(0);
  const input = yield* Input;
  yield* input.create({});
  yield* input.append({}, [1]);
});

const memory = memoryHost();

const sqlite = layerSqlite();

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect(
    "a wake runs at its recorded time, moves, then clears when the handler returns void",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const seen: Array<{ unit: Unit; items: ReadonlyArray<number> }> = [];

        const timer = Projection.process({
          ...definition,
          commit: Commit.atLeastOnce,
          process: (batch, unit) =>
            Effect.sync(() => {
              seen.push({ unit, items: batch.input.items });

              if (Option.isNone(unit.due)) return Projection.wakeAt(100);

              if (unit.due.value === 100) return Projection.wakeAt(200);

              return undefined;
            }),
        }).at({});

        const initial = yield* Projection.run(timer);
        expect(initial.wakeAt).toEqual(Option.some(100));
        yield* TestClock.adjust(99);
        expect((yield* Projection.run(timer)).units).toBe(0);
        expect(seen).toHaveLength(1);
        yield* TestClock.adjust(1);
        const first = yield* Projection.run(timer);
        expect(first).toMatchObject({ status: "caught-up", units: 1, items: 0 });
        expect(first.offsets).toEqual(initial.offsets);
        expect(first.wakeAt).toEqual(Option.some(200));
        expect(seen[1]?.unit.due).toEqual(Option.some(100));
        expect(seen[1]?.unit.ranges).toEqual({});
        expect(seen[1]?.items).toEqual([]);
        yield* TestClock.adjust(100);
        const last = yield* Projection.run(timer);
        expect(last.wakeAt).toEqual(Option.none());
        expect(seen[2]?.unit.due).toEqual(Option.some(200));
        expect(seen[2]?.unit.key).not.toBe(seen[1]?.unit.key);
        const record = Option.getOrThrow((yield* (yield* CheckpointStore).load(timer)).record);
        expect(record).not.toHaveProperty("wakeAt");
        expect(record).not.toHaveProperty("pending");
        expect((yield* Projection.run(timer)).units).toBe(0);
      }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect("input before a wake moves or cancels it; input at the wake shares one unit", () =>
    Effect.gen(function* () {
      yield* seed;
      const seen: Array<{ due: Option.Option<number>; items: ReadonlyArray<number> }> = [];

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: (batch, unit) =>
          Effect.sync(() => {
            seen.push({ due: unit.due, items: batch.input.items });
            const item = batch.input.items[0];

            if (item === 1) return Projection.wakeAt(100);

            if (item === 2) return Projection.wakeAt(200);

            if (item === 3) return Projection.wakeAt(300);

            return undefined;
          }),
      }).at({});

      const input = yield* Input;
      yield* Projection.run(timer);
      yield* TestClock.adjust(50);
      yield* input.append({}, [2]);
      expect((yield* Projection.run(timer)).wakeAt).toEqual(Option.some(200));
      expect(seen[1]?.due).toEqual(Option.none());
      yield* TestClock.adjust(150);
      yield* input.append({}, [3]);
      const combined = yield* Projection.run(timer);
      expect(combined).toMatchObject({ units: 1, items: 1 });
      expect(seen[2]).toEqual({ due: Option.some(200), items: [3] });
      yield* input.append({}, [4]);
      expect((yield* Projection.run(timer)).wakeAt).toEqual(Option.none());
      expect(seen[3]?.due).toEqual(Option.none());
    }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect("a failed pinned due unit replays its due time and key, even after new input", () =>
    Effect.gen(function* () {
      yield* seed;
      const seen: Unit[] = [];
      let fail = true;

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.gen(function* () {
            if (Option.isNone(unit.due)) return Projection.wakeAt(100);
            seen.push(unit);

            if (fail) return yield* Effect.fail("crash after pin");

            return undefined;
          }),
      }).at({});

      yield* Projection.run(timer);
      yield* TestClock.adjust(100);
      expect((yield* Projection.run(timer).pipe(Effect.result))._tag).toBe("Failure");
      const store = yield* CheckpointStore;
      const pinned = Option.getOrThrow((yield* store.load(timer)).record);
      expect(pinned.wakeAt).toBe(100);
      expect(pinned.pending).toEqual({ ranges: {}, due: 100 });
      yield* TestClock.adjust(400);
      yield* (yield* Input).append({}, [2]);
      fail = false;
      const replay = yield* Projection.run(timer, { limit: 1 });
      expect(replay).toMatchObject({ units: 1, items: 0 });
      expect(seen[1]?.key).toBe(seen[0]?.key);
      expect(seen[1]?.due).toEqual(Option.some(100));
      expect(replay.wakeAt).toEqual(Option.none());
      expect((yield* Projection.run(timer, { limit: 1 })).items).toBe(1);
    }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect(
    "run without a limit commits at most one empty due unit per cycle; pass stays one step",
    () =>
      Effect.gen(function* () {
        yield* seed;

        const timer = Projection.process({
          ...definition,
          commit: Commit.atLeastOnce,
          process: (_batch, unit) =>
            Effect.succeed(Projection.wakeAt(Option.getOrElse(unit.due, () => 0))),
        }).at({});

        // The input unit does not consume the allowance for one empty due unit.
        const first = yield* Projection.run(timer);
        expect(first).toMatchObject({ units: 2, items: 1, status: "caught-up" });
        expect(first.wakeAt).toEqual(Option.some(0));
        expect((yield* Projection.run(timer)).units).toBe(1);
        const pass = yield* Projection.pass(timer);
        expect(pass).toMatchObject({ units: 1, items: 0, status: "progress" });
        expect(pass).not.toHaveProperty("wakeAt");
      }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect("input units remain eligible after the cycle's empty due unit", () =>
    Effect.gen(function* () {
      yield* seed;
      const input = yield* Input;
      const seen: Array<ReadonlyArray<number>> = [];

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: (batch) =>
          Effect.gen(function* () {
            seen.push(batch.input.items);

            if (batch.input.items.length === 0) yield* input.append({}, [2]);

            return Projection.wakeAt(0);
          }),
      }).at({});

      const progress = yield* Projection.run(timer);
      expect(progress).toMatchObject({ units: 3, items: 2, status: "caught-up" });
      expect(seen).toEqual([[1], [], [2]]);
      expect(progress.wakeAt).toEqual(Option.some(0));
    }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect("a deadline after all inputs close still makes one empty due unit", () =>
    Effect.gen(function* () {
      yield* seed;
      const seen: Unit[] = [];

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.sync(() => {
            seen.push(unit);

            if (Option.isNone(unit.due)) return Projection.wakeAt(100);

            return undefined;
          }),
      }).at({});

      yield* (yield* Input).append({}, [], { close: true });
      expect((yield* Projection.run(timer)).status).toBe("source-closed");
      yield* TestClock.adjust(100);
      const due = yield* Projection.run(timer);
      expect(due).toMatchObject({ status: "source-closed", units: 1, items: 0 });
      expect(due.wakeAt).toEqual(Option.none());
      expect(seen[1]?.ranges).toEqual({});
      expect(seen[1]?.due).toEqual(Option.some(100));
    }),
  );
});

layer(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory)))((it) => {
  it.effect("ordinary process return values are discarded", () =>
    Effect.gen(function* () {
      yield* seed;

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: () => Effect.succeed(42),
      }).at({});

      expect((yield* Projection.run(timer)).wakeAt).toEqual(Option.none());
    }),
  );
});

layer(Protocol.bind(sqlite, Streamsy.group(Input, Accepted)).pipe(Layer.provideMerge(sqlite)))(
  (it) => {
    it.effect("a failed transactional checkpoint rolls back SQL and keeps the old wake", () =>
      Effect.gen(function* () {
        yield* seed;
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe("CREATE TABLE effects (due INTEGER NOT NULL)");

        const timer = Projection.process({
          ...definition,
          commit: Commit.transaction,
          process: (_batch, unit) =>
            Effect.gen(function* () {
              if (Option.isNone(unit.due)) return Projection.wakeAt(100);
              const client = yield* SqlClient.SqlClient;
              yield* client`INSERT INTO effects (due) VALUES (${unit.due.value})`;

              return Projection.wakeAt(200);
            }),
        }).at({});

        yield* Projection.run(timer);
        yield* TestClock.adjust(100);
        const store = yield* CheckpointStore;
        const before = yield* store.load(timer);

        const failure = new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "checkpoint failed",
        });

        const result = yield* Projection.run(timer).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: () => Effect.fail(failure),
          }),
          Effect.result,
        );

        expect(result).toMatchObject({ _tag: "Failure", failure });
        expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([]);
        expect(yield* store.load(timer)).toEqual(before);
        expect((yield* Projection.run(timer)).wakeAt).toEqual(Option.some(200));
        expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ due: 100 }]);
      }),
    );

    it.effect("accepted writes no row for a due unit and retains a returned wake request", () =>
      Effect.gen(function* () {
        yield* seed;
        const accepted = yield* Accepted;
        yield* accepted.create({});

        const timer = Projection.process({
          ...definition,
          commit: Commit.transaction,
          accepted: Accepted,
          process: (_batch, unit) =>
            Effect.succeed(Projection.wakeAt(Option.isNone(unit.due) ? 100 : 200)),
        }).at({});

        expect((yield* Projection.run(timer)).wakeAt).toEqual(Option.some(100));
        const before = yield* accepted.head({});
        yield* TestClock.adjust(100);
        const due = yield* Projection.run(timer);
        expect(due).toMatchObject({ units: 1, items: 0 });
        expect(due.wakeAt).toEqual(Option.some(200));
        expect(yield* accepted.head({})).toEqual(before);
      }),
    );
  },
);

test("wakeAt validates epoch milliseconds and converts DateTime", () => {
  expect(Projection.wakeAt(0).wakeAt).toBe(0);
  expect(Projection.wakeAt(DateTime.makeUnsafe(1000)).wakeAt).toBe(1000);

  for (const invalid of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => Projection.wakeAt(invalid)).toThrow(RangeError);
  }
});

test("non-timer unit keys retain their stored encoding", () => {
  const unit = unitOf("timer", 1, 0, {}, {});
  expect(unit.key).toBe(JSON.stringify(["timer", "0", "{}", "{}"]));
  expect(unit.due).toEqual(Option.none());
  expect(unitOf("timer", 1, 0, {}, {}, 100).key).not.toBe(unit.key);
});
