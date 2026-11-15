import { expect, layer } from "@effect/vitest";
import { Array, Clock, Context, Effect, Layer, Option, Schedule, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Storage, StorageFault, StreamId } from "@streamsy/core";
import { AlarmSlot } from "../../src/cloudflare/alarm.ts";

import { makeWakeups, Wakeups, type WakeupHost } from "../../src/cloudflare/wakeup.ts";

const fakeStorage = (next: Option.Option<{ readonly at: number; readonly streamId: string }>) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: () => Effect.succeedNone,
    messages: () => Effect.succeed([]),
    producer: () => Effect.succeedNone,
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    committed: Effect.die("unexpected committed subscription"),
    nextExpiry: Effect.succeed(
      Option.isSome(next)
        ? Option.some({ ...next.value, streamId: StreamId.make(next.value.streamId) })
        : Option.none(),
    ),
  });

const cases = [
  [Option.none(), Option.some(10_000), ["clear"]],
  [Option.some({ at: 10_000, streamId: "a" }), Option.none(), [10_000]],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(10_000), []],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(9_000), []],
  [Option.some({ at: 10_000, streamId: "a" }), Option.some(11_000), [10_000]],
  [Option.some({ at: 0, streamId: "a" }), Option.some(0), ["clear", 1]],
  [Option.some({ at: 0, streamId: "a" }), Option.none(), [1]],
] as const;

for (const [index, [next, current, expected]] of cases.entries()) {
  const actions: Array<string | number> = [];
  layer(
    Layer.mergeAll(
      Layer.succeed(Storage, fakeStorage(next)),
      Layer.succeed(AlarmSlot, {
        current: Effect.succeed(current),
        arm: (at) => Effect.sync(() => actions.push(at)),
        clear: Effect.sync(() => actions.push("clear")),
      }),
    ),
  )((it) => {
    it.effect(`reconcileAlarm follows the indexed deadline table (row ${index})`, () =>
      Effect.gen(function* () {
        const context = yield* Effect.context<Storage | AlarmSlot>();
        yield* makeWakeups(context);
      }).pipe(
        Effect.andThen(() =>
          Effect.sync(() => {
            expect(actions).toEqual([...expected]);
          }),
        ),
      ),
    );
  });
}

layer(
  Layer.mergeAll(
    Layer.succeed(Storage, fakeStorage(Option.none())),
    Layer.succeed(AlarmSlot, {
      current: Effect.succeedNone,
      arm: () => Effect.void,
      clear: Effect.void,
    }),
  ),
)((it) => {
  it.effect(
    "a kept alarm retains its requested retry deadline when reconciliation runs later",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const context = yield* Effect.context<Storage | AlarmSlot>();
        let host: WakeupHost | undefined;
        let armed = Option.none<number>();
        let requested = false;
        let arms = 0;

        const scheduler = yield* makeWakeups(
          Context.add(context, AlarmSlot, {
            current: Effect.sync(() => armed),
            arm: (at) =>
              Effect.sync(() => {
                armed = Option.some(at);
                arms += 1;
              }),
            clear: Effect.sync(() => {
              armed = Option.none();
            }),
          }),
          [
            {
              name: "retained-request",
              retry: Schedule.spaced("1 hour"),
              // Moving future work allows a later reconciliation to retain the earlier alarm.
              next: Clock.currentTimeMillis.pipe(
                Effect.map((now) => Option.some(requested ? now + 100 : 0)),
              ),
              run: Effect.fail("offline"),
              bind: (bound) =>
                Effect.sync(() => {
                  host = bound;
                }),
            },
          ],
        );

        yield* scheduler.alarm;
        requested = true;
        yield* host!.retry;
        yield* host!.reconcile;
        expect(armed).toEqual(Option.some(100));
        expect((yield* host!.state).retryAt).toEqual(armed);
        const before = arms;
        yield* TestClock.adjust(1);
        yield* host!.reconcile;
        expect(arms).toBe(before);
        expect(armed).toEqual(Option.some(100));
        expect((yield* host!.state).retryAt).toEqual(armed);
        yield* TestClock.adjust(100);
        yield* host!.reconcile;
        expect(armed).toEqual(Option.some(201));
        expect((yield* host!.state).retryAt).toEqual(armed);
      }).pipe(Effect.ensuring(TestClock.setTime(0))),
  );

  it.effect("status reports each wakeup's deferred count, 0 when it has none", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();

      const { context: object } = yield* makeWakeups(context, [
        {
          name: "with-deferred",
          next: Effect.succeedNone,
          run: Effect.void,
          deferred: Effect.succeed(3),
        },
      ]);

      const status = yield* Effect.flatMap(Wakeups, (wakeups) => wakeups.status).pipe(
        Effect.provideContext(object),
      );

      expect(Array.map(status, ({ name, deferred }) => [name, deferred])).toEqual([
        ["streamsy/expiry", 0],
        ["with-deferred", 3],
      ]);
    }),
  );

  it.effect("bind runs once and reconcile inside a wakeup run does not deadlock", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      let host: WakeupHost | undefined;
      let binds = 0;
      let pending = true;

      const scheduler = yield* makeWakeups(context, [
        {
          name: "bound",
          next: Effect.sync(() => (pending ? Option.some(0) : Option.none())),
          bind: (bound) =>
            Effect.sync(() => {
              host = bound;
              binds += 1;
            }),
          run: Effect.suspend(() => {
            pending = false;

            return host!.reconcile;
          }),
        },
      ]);

      expect(binds).toBe(1);
      yield* host!.reconcile;
      yield* scheduler.alarm;
      expect(pending).toBe(false);
      expect(binds).toBe(1);
      expect(yield* host!.state).toEqual({
        failures: 0,
        retryAt: Option.none(),
      });
    }),
  );

  it.effect("bound state exposes storage backoff and retry clears it", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      let host: WakeupHost | undefined;

      const scheduler = yield* makeWakeups(context, [
        {
          name: "failing",
          next: Effect.succeedSome(0),
          retry: Schedule.spaced("10 seconds"),
          bind: (bound) =>
            Effect.sync(() => {
              host = bound;
            }),
          run: Effect.fail(
            new StorageFault({ operation: "test", message: "offline", retryable: true }),
          ),
        },
      ]);

      yield* scheduler.alarm;
      expect(yield* host!.state).toEqual({
        failures: 1,
        retryAt: Option.some(10_000),
      });
      yield* host!.reconcile;
      yield* host!.retry;
      expect(yield* host!.state).toEqual({
        failures: 1,
        retryAt: Option.none(),
      });
    }),
  );

  it.effect("host retry clears application backoff and reconcile arms immediate pending work", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      let host: WakeupHost | undefined;

      const scheduler = yield* makeWakeups(context, [
        {
          name: "application-failure",
          next: Effect.succeedSome(0),
          retry: Schedule.spaced("1 hour"),
          bind: (bound) =>
            Effect.sync(() => {
              host = bound;
            }),
          run: Effect.fail("binding failed"),
        },
      ]);

      yield* scheduler.alarm;
      expect(yield* host!.state).toEqual({
        failures: 1,
        retryAt: Option.some(3_600_000),
      });
      yield* host!.retry;
      yield* host!.reconcile;
      expect(yield* host!.state).toEqual({
        failures: 1,
        retryAt: Option.some(1),
      });
    }),
  );

  it.effect("host retry clears a deadline-read floor without resetting the policy", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      let host: WakeupHost | undefined;
      yield* makeWakeups(context, [
        {
          name: "deadline-failure",
          retry: Schedule.exponential("1 hour"),
          next: Effect.fail(
            new StorageFault({ operation: "test.next", message: "offline", retryable: true }),
          ),
          bind: (bound) =>
            Effect.sync(() => {
              host = bound;
            }),
          run: Effect.void,
        },
      ]);
      yield* host!.retry;
      expect(yield* host!.state).toEqual({ failures: 1, retryAt: Option.none() });
      yield* host!.reconcile;
      expect(yield* host!.state).toEqual({
        failures: 2,
        retryAt: Option.some(7_200_000),
      });
    }),
  );

  it.effect("changed runs after a run or failed deadline read updates retry state", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      const observed: Array<string> = [];

      const scheduler = yield* makeWakeups(context, [
        ...Array.map(["idle", "future", "backoff", "exhausted", "due"], (name) => ({
          name,
          next:
            name === "idle"
              ? Effect.succeedNone
              : Effect.succeedSome(name === "future" ? 10_000 : 0),
          retry: name === "exhausted" ? Schedule.recurs(0) : Schedule.spaced("1 hour"),
          run: name === "due" ? Effect.void : Effect.fail("offline"),
          changed: Effect.sync(() => {
            observed.push(name);
          }),
        })),
        {
          name: "unreadable",
          next: Effect.fail(
            new StorageFault({ operation: "test.next", message: "offline", retryable: true }),
          ),
          run: Effect.die("must not run"),
          changed: Effect.sync(() => {
            observed.push("unreadable");
          }),
        },
      ]);

      yield* scheduler.alarm;
      expect(observed).toEqual(["unreadable", "backoff", "exhausted", "due"]);
      observed.length = 0;
      yield* scheduler.alarm;
      expect(observed).toEqual([]);
    }),
  );

  it.effect("changed sees the updated retry state and a broken observer does not stop peers", () =>
    Effect.gen(function* () {
      const context = yield* Effect.context<Storage | AlarmSlot>();
      let host: WakeupHost | undefined;
      let observed = 0;
      let peerRan = false;

      const scheduler = yield* makeWakeups(context, [
        {
          name: "observed",
          next: Effect.succeedSome(0),
          bind: (bound) =>
            Effect.sync(() => {
              host = bound;
            }),
          run: Effect.fail("offline"),
          changed: Effect.suspend(() => host!.state).pipe(
            Effect.tap((state) =>
              Effect.sync(() => {
                observed = state.failures;
              }),
            ),
            Effect.andThen(Effect.die("observer broke")),
          ),
        },
        {
          name: "peer",
          next: Effect.succeedSome(0),
          run: Effect.sync(() => {
            peerRan = true;
          }),
        },
      ]);

      yield* scheduler.alarm;
      expect(observed).toBe(1);
      expect(peerRan).toBe(true);
    }),
  );
});
