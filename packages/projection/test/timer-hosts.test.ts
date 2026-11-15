/* oxlint-disable effecttsgo/strict-effect-provide -- Each case builds a fresh storage graph; the suite clock stays shared with its runner. */
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Clock, Cron, Effect, Exit, Layer, Match, Option, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol, Streams } from "@streamsy/core/protocol";
import {
  Commit,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
  type Attachable,
} from "@streamsy/projection";
import type { Executable } from "../src/executable.ts";
import type { InputMap } from "../src/batch.ts";
import { memoryHost } from "./support/memory-host.ts";
import { layerSqlite } from "./support/sqlite.ts";
import { eventually } from "./support/eventually.ts";

class Input extends Streamsy.json<Input>()("TimerHosts/Input", "timer-hosts/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Fired extends Streamsy.json<Fired>()("TimerHosts/Fired", "timer-hosts/fired", {
  params: {},
  schema: Schema.String,
}) {}

const definition = { id: "host-timer", params: {}, inputs: { input: Input } };

const seed = Effect.gen(function* () {
  yield* TestClock.setTime(0);
  const input = yield* Input;
  yield* input.create({});
  yield* input.append({}, [100]);
});

/** Both hosts share a runner; the alarm adapter calls it only when the host ticks. */
const launch = <I extends InputMap, E, R, Q, A, P, OR>(
  declaration: Attachable<Executable<I, E, R, Q, A, P, unknown, OR>>,
  alarm: boolean,
  durable = true,
) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));

    const tick = alarm
      ? (yield* ProjectionWakeup.attach(declaration, { durable }).pipe(
          Scope.provide(scope),
        )).run.pipe(Effect.asVoid)
      : (yield* Layer.build(Projection.attach(declaration, { durable })).pipe(Scope.provide(scope)),
        Effect.yieldNow);

    return { tick, close: Scope.close(scope, Exit.void) };
  });

const settle = <E, R>(tick: Effect.Effect<void, E, R>) =>
  Effect.gen(function* () {
    const registry = yield* Projection.Registry;

    for (let n = 0; n < 1_000; n += 1) {
      yield* tick;
      const status = (yield* registry.status)[0]!;

      if (status.active === 0 && status.queued === 0 && status.marks.fresh === 0) return status;
      yield* Effect.yieldNow;
    }

    return (yield* registry.status)[0]!;
  });

for (const [storage, local] of [
  ["memory", memoryHost()],
  ["sqlite", layerSqlite()],
] as const) {
  const host = Projection.Registry.layer.pipe(
    Layer.provideMerge(
      Protocol.bind(local, Streamsy.group(Input, Fired)).pipe(Layer.provideMerge(local)),
    ),
  );

  layer(host)((it) => {
    for (const alarm of [false, true]) {
      const label = `${storage}, alarm=${alarm}`;
      it.effect(`snooze moves, cancels, wakes and reports event/status (${label})`, () =>
        Effect.gen(function* () {
          yield* seed;
          const due: number[] = [];

          const timer = Projection.process({
            ...definition,
            commit: Commit.atLeastOnce,
            process: (batch, unit) =>
              Effect.sync(() => {
                if (Option.isSome(unit.due)) due.push(unit.due.value);
                const time = batch.input.items.at(-1);

                return time === undefined || time === 0 ? undefined : Projection.wakeAt(time);
              }),
          });

          const registry = yield* Projection.Registry;
          const events = yield* Stream.toPull(registry.events);
          const { tick } = yield* launch(timer, alarm);
          yield* registry.schedule(timer.at({}));
          expect((yield* settle(tick)).marks).toMatchObject({
            sleeping: 1,
            fresh: 0,
            nextDueAt: Option.some(100),
          });
          expect((yield* events).at(-1)).toMatchObject({ _tag: "Ran", wakeAt: Option.some(100) });
          yield* TestClock.adjust(50);
          const input = yield* Input;
          yield* input.append({}, [200]);
          yield* settle(tick);
          expect((yield* registry.status)[0]!.marks.nextDueAt).toEqual(Option.some(200));
          yield* TestClock.adjust(50);
          yield* settle(tick);
          expect(due).toEqual([]);
          yield* input.append({}, [0]);
          expect((yield* settle(tick)).marks.sleeping).toBe(0);
          yield* TestClock.adjust(100);
          yield* settle(tick);
          expect(due).toEqual([]);
          yield* input.append({}, [300]);
          yield* settle(tick);
          yield* TestClock.adjust(100);
          expect((yield* settle(tick)).marks.sleeping).toBe(0);
          expect(due).toEqual([300]);
        }).pipe(Effect.provide(Layer.fresh(host))),
      );

      it.effect(
        `continue-confirm sees its own append before its confirmation wake (${label})`,
        () =>
          Effect.gen(function* () {
            yield* seed;
            const seen: number[][] = [];
            let confirmations = 0;

            const timer = Projection.process({
              ...definition,
              commit: Commit.atLeastOnce,
              process: (batch, unit) =>
                Effect.gen(function* () {
                  seen.push([...batch.input.items]);

                  if (batch.input.items.includes(100)) yield* (yield* Input).append({}, [101]);

                  if (Option.isSome(unit.due)) {
                    confirmations += 1;

                    return undefined;
                  }

                  return Projection.wakeAt(500);
                }),
            });

            const { tick } = yield* launch(timer, alarm);
            yield* (yield* Projection.Registry).schedule(timer.at({}));
            yield* settle(tick);
            expect(seen).toEqual([[100], [101]]);
            yield* TestClock.adjust(500);
            yield* settle(tick);
            expect(confirmations).toBe(1);
            expect(seen).toEqual([[100], [101], []]);
          }).pipe(Effect.provide(Layer.fresh(host))),
      );

      it.effect(`agent heartbeat postpones stale (${label})`, () =>
        Effect.gen(function* () {
          yield* seed;
          const stale: number[] = [];

          const timer = Projection.process({
            ...definition,
            commit: Commit.atLeastOnce,
            process: (_batch, unit) =>
              Effect.gen(function* () {
                if (Option.isSome(unit.due)) {
                  stale.push(unit.due.value);

                  return undefined;
                }

                return Projection.wakeAt((yield* Clock.currentTimeMillis) + 1_000);
              }),
          });

          const { tick } = yield* launch(timer, alarm);
          yield* (yield* Projection.Registry).schedule(timer.at({}));
          yield* settle(tick);

          for (let n = 0; n < 3; n += 1) {
            yield* TestClock.adjust(500);
            yield* (yield* Input).append({}, [n]);
            expect((yield* settle(tick)).marks.nextDueAt).toEqual(Option.some(1_500 + n * 500));
          }

          yield* TestClock.adjust(999);
          yield* settle(tick);
          expect(stale).toEqual([]);
          yield* TestClock.adjust(1);
          yield* settle(tick);
          expect(stale).toEqual([2_500]);
        }).pipe(Effect.provide(Layer.fresh(host))),
      );

      it.effect(
        `fresh mark after a committed checkpoint sleeps without rerunning handler (${label})`,
        () =>
          Effect.gen(function* () {
            yield* seed;
            let runs = 0;

            const timer = Projection.process({
              ...definition,
              commit: Commit.atLeastOnce,
              process: (_batch, unit) =>
                Effect.sync(() => {
                  runs += 1;

                  return Option.isNone(unit.due) ? Projection.wakeAt(100) : undefined;
                }),
            });

            // The committed checkpoint and fresh mark model a crash before marks.sleep.
            yield* Projection.run(timer.at({}));
            const marks = yield* ProjectionMarks;
            yield* Effect.orDie(marks.mark(timer.id, ["{}"]));
            const { tick } = yield* launch(timer, alarm);
            expect((yield* settle(tick)).marks.sleeping).toBe(1);
            expect(runs).toBe(1);
            yield* TestClock.adjust(100);
            yield* settle(tick);
            expect(runs).toBe(2);
          }).pipe(Effect.provide(Layer.fresh(host))),
      );

      for (const stop of [false, true]) {
        it.effect(
          `wake waits for ${stop ? "registry.retry after stop" : "backoff"} (${label})`,
          () =>
            Effect.gen(function* () {
              yield* seed;
              let broken = false;
              const due: number[] = [];

              const timer = Projection.process({
                ...definition,
                commit: Commit.atLeastOnce,
                process: (_batch, unit) =>
                  Effect.gen(function* () {
                    if (broken)
                      return yield* Effect.fail(
                        stop
                          ? new ProjectionFault({
                              phase: "process",
                              reason: "invalid-output",
                              message: "bad",
                            })
                          : "offline",
                      );

                    if (Option.isSome(unit.due)) {
                      due.push(unit.due.value);

                      return undefined;
                    }

                    return Projection.wakeAt(100);
                  }),
              });

              const registry = yield* Projection.Registry;
              const { tick } = yield* launch(timer, alarm);
              yield* registry.schedule(timer.at({}));
              yield* settle(tick);
              yield* TestClock.adjust(50);
              broken = true;
              yield* (yield* Input).append({}, [2]);
              expect((yield* settle(tick)).marks).toMatchObject({
                sleeping: 0,
                backingOff: stop ? 0 : 1,
                stopped: stop ? 1 : 0,
              });
              broken = false;
              yield* TestClock.adjust(50);
              yield* settle(tick);
              expect(due).toEqual([]);

              if (stop) {
                expect(yield* registry.retry(timer)).toBe(1);
              } else yield* TestClock.adjust(950);
              yield* settle(tick);
              expect(due).toEqual([100]);
            }).pipe(Effect.provide(Layer.fresh(host))),
        );
      }

      for (const durable of [false, true]) {
        it.effect(`sleeping timer restart (durable=${durable}, ${label})`, () =>
          Effect.gen(function* () {
            yield* seed;
            let wakes = 0;

            const timer = Projection.process({
              ...definition,
              commit: Commit.atLeastOnce,
              process: (_batch, unit) =>
                Effect.sync(() => {
                  if (Option.isSome(unit.due)) {
                    wakes += 1;

                    return undefined;
                  }

                  return Projection.wakeAt(100);
                }),
            });

            const first = yield* launch(timer, alarm, durable);
            yield* (yield* Projection.Registry).schedule(timer.at({}));
            expect((yield* settle(first.tick)).marks.sleeping).toBe(1);
            yield* first.close;
            const second = yield* launch(timer, alarm, durable);
            yield* TestClock.adjust(100);
            yield* settle(second.tick);
            expect(wakes).toBe(durable ? 1 : 0);

            if (!durable) {
              yield* (yield* Input).append({}, [2]);
              yield* settle(second.tick);
              expect(wakes).toBe(1);
            }
          }).pipe(Effect.provide(Layer.fresh(host))),
        );
      }
    }
  });
}

const sql = layerSqlite();

const sqlHost = Projection.Registry.layer.pipe(
  Layer.provideMerge(
    Protocol.bind(sql, Streamsy.group(Input, Fired)).pipe(Layer.provideMerge(sql)),
  ),
);

layer(sqlHost)((it) => {
  const host = sqlHost;

  for (const alarm of [false, true]) {
    for (const policy of ["one-shot", "skip", "once", "all"] as const) {
      it.effect(
        `transactional timer rows: ${policy} including scheduleClock upsert (alarm=${alarm})`,
        () =>
          Effect.gen(function* () {
            yield* seed;
            const client = yield* SqlClient.SqlClient;
            yield* client`CREATE TABLE timers (timer_key TEXT PRIMARY KEY, due_at BIGINT NOT NULL)`;
            const daily = Cron.parseUnsafe("0 0 * * *", "UTC");
            const day = 86_400_000;
            const first = policy === "one-shot" ? 100 : day;
            const fired = yield* Fired;
            yield* fired.create({});

            const timer = Projection.process({
              ...definition,
              commit: Commit.transaction,
              outputs: { fired: Fired },
              process: (batch, unit, { outputs }) =>
                Effect.gen(function* () {
                  const tx = yield* SqlClient.SqlClient;

                  // WorkflowEngine.scheduleClock's boundary is just an upsert fact.
                  for (const dueAt of batch.input.items) {
                    yield* tx`INSERT INTO timers (timer_key, due_at) VALUES ('clock', ${dueAt})
                ON CONFLICT (timer_key) DO UPDATE SET due_at = excluded.due_at`;
                  }

                  const now = yield* Clock.currentTimeMillis;

                  if (Option.isSome(unit.due)) {
                    const rows = yield* tx<{
                      timer_key: string;
                      due_at: number;
                    }>`SELECT * FROM timers WHERE due_at <= ${unit.due.value}`;

                    for (const row of rows) {
                      let dueAt = row.due_at;

                      if (policy !== "skip") {
                        do {
                          yield* Streams.append(outputs.fired, [`${row.timer_key}@${dueAt}`]);

                          if (policy !== "all") break;
                          dueAt = Cron.next(daily, dueAt).getTime();
                        } while (dueAt <= now);
                      }

                      if (policy === "one-shot")
                        yield* tx`DELETE FROM timers WHERE timer_key = ${row.timer_key}`;
                      else {
                        const next = Cron.next(daily, now).getTime();
                        yield* tx`UPDATE timers SET due_at = ${next} WHERE timer_key = ${row.timer_key}`;
                      }
                    }
                  }

                  const rows = yield* tx<{
                    at: number | null;
                  }>`SELECT MIN(due_at) AS at FROM timers`;

                  return rows[0]?.at == null ? undefined : Projection.wakeAt(rows[0].at);
                }),
            });

            let wall = 0;
            const real = yield* Clock.Clock;

            const clock: Clock.Clock = {
              monotonicTimeNanos: real.monotonicTimeNanos,
              monotonicTimeNanosUnsafe: () => real.monotonicTimeNanosUnsafe(),
              currentTimeNanos: real.currentTimeNanos,
              currentTimeNanosUnsafe: () => real.currentTimeNanosUnsafe(),
              sleep: (duration) => real.sleep(duration),
              currentTimeMillisUnsafe: () => wall,
              currentTimeMillis: Effect.sync(() => wall),
            };

            const launched = yield* launch(timer, alarm).pipe(
              Effect.provideService(Clock.Clock, clock),
            );

            const tick = launched.tick.pipe(Effect.provideService(Clock.Clock, clock));
            yield* (yield* Input).append({}, [first]);
            yield* (yield* Projection.Registry).schedule(timer.at({}));
            expect((yield* settle(tick)).marks.nextDueAt).toEqual(Option.some(first));

            if (policy === "one-shot") {
              wall = 50;
              yield* TestClock.adjust(50);
              yield* (yield* Input).append({}, [200]);
              expect((yield* settle(tick)).marks.nextDueAt).toEqual(Option.some(200));
              wall = 200;
              yield* TestClock.adjust(150);
            } else {
              wall = 3 * day + 1_000;
              yield* TestClock.adjust(30_000);
            }

            const status = yield* settle(tick);

            const expected = Match.value(policy).pipe(
              Match.when("skip", () => []),
              Match.when("all", () => [`clock@${day}`, `clock@${2 * day}`, `clock@${3 * day}`]),
              Match.when("one-shot", () => ["clock@200"]),
              Match.when("once", () => [`clock@${day}`]),
              Match.exhaustive,
            );

            expect((yield* fired.collect({})).items).toEqual(expected);
            expect(status.marks.sleeping).toBe(policy === "one-shot" ? 0 : 1);

            if (policy !== "one-shot") expect(status.marks.nextDueAt).toEqual(Option.some(4 * day));
          }).pipe(Effect.provide(Layer.fresh(host))),
      );
    }
  }
});

const memory = memoryHost();

const waitHost = Projection.Registry.layer.pipe(
  Layer.provideMerge(Protocol.bind(memory, Streamsy.group(Input)).pipe(Layer.provideMerge(memory))),
);

layer(waitHost)((it) => {
  const host = waitHost;
  it.effect("process wait re-reads wall time after the 30 second cap and a 2 hour jump", () =>
    Effect.gen(function* () {
      yield* seed;
      let wall = 0;
      let wakes = 0;
      let reads = 0;
      const real = yield* Clock.Clock;

      const clock: Clock.Clock = {
        monotonicTimeNanos: real.monotonicTimeNanos,
        monotonicTimeNanosUnsafe: () => real.monotonicTimeNanosUnsafe(),
        currentTimeNanos: real.currentTimeNanos,
        currentTimeNanosUnsafe: () => real.currentTimeNanosUnsafe(),
        sleep: (duration) => real.sleep(duration),
        currentTimeMillisUnsafe: () => wall,
        currentTimeMillis: Effect.sync(() => wall),
      };

      const timer = Projection.process({
        ...definition,
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          Effect.sync(() => {
            if (Option.isSome(unit.due)) {
              wakes += 1;

              return undefined;
            }

            return Projection.wakeAt(3_600_000);
          }),
      });

      const marks = yield* ProjectionMarks;
      yield* Layer.build(Projection.attach(timer, { durable: true })).pipe(
        Effect.provideService(Clock.Clock, clock),
        Effect.provideService(ProjectionMarks, {
          ...marks,
          nextDue: (id) =>
            Effect.suspend(() => {
              reads += 1;

              return marks.nextDue(id);
            }),
        }),
      );
      yield* (yield* Projection.Registry).schedule(timer.at({}));
      yield* settle(Effect.yieldNow);
      yield* eventually(
        Effect.sync(() => reads),
        (n) => n > 0,
      );
      wall = 7_200_000;
      yield* TestClock.adjust(29_999);
      expect(wakes).toBe(0);
      yield* TestClock.adjust(1);
      yield* settle(Effect.yieldNow);
      expect(wakes).toBe(1);
      expect(reads).toBeGreaterThan(1);
    }).pipe(Effect.provide(Layer.fresh(host))),
  );
});
