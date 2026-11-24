/* oxlint-disable effecttsgo/strict-effect-provide -- Each test owns the input and can instrument its wire service. */
import { expect, it } from "@effect/vitest";
import { DurableState, MemoryStorage, StorageFault, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { CheckpointStore, ProjectionRetry } from "@streamsy/projection";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Schedule,
  Scope,
  Stream,
} from "effect";
import { View } from "../../src/index.ts";
import { goldOrders, orders, Shop } from "../circuit/support/shop.ts";
import { hostFor, sqlite } from "../support/backends.ts";
import { memoryDump } from "../support/memory.ts";
import {
  append,
  awaitMatch,
  createShop,
  liveHost,
  liveOrders,
  params,
  putOrder,
} from "./support.ts";

const inputHost = MemoryStorage.layer(Streamsy.group(Shop));

it.live("replays independently for two readers with the same parameters", () =>
  Effect.gen(function* () {
    yield* createShop;
    const { offset } = yield* putOrder(2);
    const firstScope = yield* Scope.fork(yield* Effect.scope);
    const first = yield* liveOrders.at(params).pipe(Scope.provide(firstScope));
    const second = yield* liveOrders.at(params);

    yield* first.caughtUp;
    yield* second.caughtUp;
    const initial = yield* first.snapshot;
    expect(initial.offsets[Shop.key]).toBe(offset);
    expect(yield* second.snapshot).toEqual(initial);
    expect((yield* second.snapshot).rows.orders).not.toBe(initial.rows.orders);

    yield* Scope.close(firstScope, Exit.void);
    const next = yield* putOrder(3);
    yield* awaitMatch(second.snapshot, ({ offsets }) => offsets[Shop.key] === next.offset);
    expect(yield* first.snapshot).toBe(initial);
    expect((yield* second.snapshot).rows.orders[0]?.total).toBe(3);
  }).pipe(Effect.provide(inputHost)),
);

it.live("builds a fresh memory host even under an ambient memory Views host", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(2);
    const first = yield* liveOrders.at(params);
    yield* first.caughtUp;
    const second = yield* liveOrders.at(params);
    yield* second.caughtUp;

    expect(yield* second.snapshot).toEqual(yield* first.snapshot);
    const checkpoints = yield* CheckpointStore;
    expect((yield* checkpoints.load({ id: "live-orders", params, generation: 1 })).token).toBe("0");
    expect((yield* memoryDump).integrals).toEqual([]);
  }).pipe(Effect.provide(liveHost)),
);

it.live("reads State from an ambient SQL host using private heap state", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(2);

    const reader = yield* liveOrders.at(params);
    yield* reader.caughtUp;
    expect((yield* reader.snapshot).rows.orders[0]?.total).toBe(2);
  }).pipe(Effect.provide(hostFor(sqlite, Streamsy.group(Shop)))),
);

it.live("keeps the last committed rows and offsets when an operator rejects a unit", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* append([
      DurableState.upsert("customer", { customerId: "c1", name: "Name", tier: "gold" }),
    ]);
    yield* putOrder(1);

    const definition = {
      id: "live-operator-fault",
      params: Shop.paramCodecs,
      views: { orders, goldOrders },
    };

    const live = yield* View.live({ ...definition, maxAffectedKeys: 12 }).at(params);
    yield* live.caughtUp;

    for (const key of ["o2", "o3", "o4"]) {
      const { offset } = yield* putOrder(1, key);
      yield* awaitMatch(live.snapshot, ({ offsets }) => offsets[Shop.key] === offset);
    }

    const initial = yield* live.snapshot;
    expect(initial.rows.goldOrders).toHaveLength(4);

    const rejected = yield* append([
      DurableState.upsert("customer", { customerId: "c1", name: "Renamed", tier: "gold" }),
      DurableState.upsert("order", { orderId: "o1", customerId: "c1", total: 2 }),
    ]);

    expect(yield* live.snapshots.pipe(Stream.runDrain, Effect.flip)).toMatchObject({
      _tag: "ViewDataFault",
      reason: expect.stringContaining("maxAffectedKeys (12) exceeded"),
    });
    expect(yield* live.snapshot).toBe(initial);

    const replay = yield* View.live(definition).at(params);
    yield* replay.caughtUp;
    const latest = yield* replay.snapshot;
    expect(latest.offsets[Shop.key]).toBe(rejected.offset);
    expect(latest.rows.orders[0]?.total).toBe(2);
    expect(latest.rows.goldOrders[0]?.customerName).toBe("Renamed");
  }).pipe(Effect.provide(inputHost)),
);

it.live(
  "interrupts the input read and ends an outside subscription when its reader scope closes",
  () =>
    Effect.gen(function* () {
      yield* createShop;
      yield* putOrder(1);
      const shop = yield* Shop;
      const entered = yield* Deferred.make<void>();
      const exited = yield* Deferred.make<void>();
      const scope = yield* Scope.fork(yield* Effect.scope);

      const live = yield* liveOrders.at(params).pipe(
        Scope.provide(scope),
        Effect.provideService(Shop, {
          ...shop,
          wire: {
            ...shop.wire,
            read: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(exited, undefined)),
              ),
          },
        }),
      );

      yield* Deferred.await(entered);
      const subscribed = yield* Deferred.make<void>();

      const subscription = yield* live.snapshots.pipe(
        Stream.tap(() => Deferred.succeed(subscribed, undefined)),
        Stream.runCollect,
        Effect.forkScoped,
      );

      yield* Deferred.await(subscribed);
      const initial = yield* live.snapshot;
      expect(initial.rows.orders).toEqual([]);
      yield* Scope.close(scope, Exit.void);
      expect(yield* Deferred.isDone(exited)).toBe(true);
      expect(yield* Fiber.join(subscription)).toEqual([initial]);
      expect(yield* live.snapshots.pipe(Stream.runCollect)).toEqual([initial]);

      const replay = yield* liveOrders.at(params);
      yield* replay.caughtUp;
      expect((yield* replay.snapshot).rows.orders).toHaveLength(1);
    }).pipe(Effect.provide(inputHost)),
);

it.live("interrupts the input head read when the reader closes before its first unit", () =>
  Effect.gen(function* () {
    yield* createShop;
    const shop = yield* Shop;
    const entered = yield* Deferred.make<void>();
    const exited = yield* Deferred.make<void>();
    const scope = yield* Scope.fork(yield* Effect.scope);

    yield* liveOrders.at(params).pipe(
      Scope.provide(scope),
      Effect.provideService(Shop, {
        ...shop,
        wire: {
          ...shop.wire,
          head: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(exited, undefined)),
            ),
        },
      }),
    );

    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    expect(yield* Deferred.isDone(exited)).toBe(true);

    const replay = yield* liveOrders.at(params);
    yield* replay.caughtUp;
    expect((yield* replay.snapshot).rows.orders).toEqual([]);
  }).pipe(Effect.provide(inputHost)),
);

const catchUpBackoff = Effect.gen(function* () {
  const shop = yield* Shop;
  const ambientClock = yield* Clock.Clock;
  const sleeping = yield* Deferred.make<void>();
  const stopped = yield* Deferred.make<void>();

  const offline = new StorageFault({
    operation: "head",
    message: "head remains unavailable",
    retryable: true,
  });

  const clock: Clock.Clock = {
    currentTimeMillis: ambientClock.currentTimeMillis,
    currentTimeMillisUnsafe: () => ambientClock.currentTimeMillisUnsafe(),
    currentTimeNanos: ambientClock.currentTimeNanos,
    currentTimeNanosUnsafe: () => ambientClock.currentTimeNanosUnsafe(),
    monotonicTimeNanos: ambientClock.monotonicTimeNanos,
    monotonicTimeNanosUnsafe: () => ambientClock.monotonicTimeNanosUnsafe(),
    sleep: (duration: Duration.Duration) =>
      Duration.toMillis(duration) !== 3_600_000
        ? ambientClock.sleep(duration)
        : Deferred.succeed(sleeping, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(stopped, undefined)),
          ),
  };

  const open = (wire = shop.wire) =>
    liveOrders.at(params).pipe(
      Effect.provideService(Shop, {
        ...shop,
        wire: { ...wire, head: () => Effect.fail(offline) },
      }),
      Effect.provideService(Clock.Clock, clock),
      Effect.provideService(ProjectionRetry, Schedule.spaced("1 hour")),
    );

  return { open, sleeping, stopped };
});

it.live("a fatal follower fault interrupts the catch-up backoff and fails its waiters", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(1);
    const shop = yield* Shop;
    const { open, sleeping, stopped } = yield* catchUpBackoff;
    const failRead = yield* Deferred.make<void>();
    const fault = new StorageFault({ operation: "read", message: "fatal read", retryable: false });

    const live = yield* open({
      ...shop.wire,
      read: () => Deferred.await(failRead).pipe(Effect.andThen(fault)),
    });

    yield* Deferred.await(sleeping);

    const ready = yield* live.caughtUp.pipe(
      Effect.flip,
      Effect.forkScoped({ startImmediately: true }),
    );

    yield* Deferred.succeed(failRead, undefined);
    const error = yield* Fiber.join(ready);
    expect(error).toMatchObject({ reason: "storage-failure", cause: fault });
    expect(yield* live.snapshots.pipe(Stream.runDrain, Effect.flip)).toBe(error);
    expect(yield* Deferred.isDone(stopped)).toBe(true);
    expect((yield* live.snapshot).rows.orders).toEqual([]);
  }).pipe(Effect.provide(inputHost)),
);

it.live("scope close interrupts and joins the catch-up backoff", () =>
  Effect.gen(function* () {
    yield* createShop;
    const { open, sleeping, stopped } = yield* catchUpBackoff;
    const scope = yield* Scope.fork(yield* Effect.scope);
    const live = yield* open().pipe(Scope.provide(scope));

    yield* Deferred.await(sleeping);
    const ready = yield* live.caughtUp.pipe(Effect.forkScoped({ startImmediately: true }));

    yield* Scope.close(scope, Exit.void);
    expect(yield* Deferred.isDone(stopped)).toBe(true);
    const exit = yield* Fiber.await(ready);
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);

    const reopened = yield* liveOrders.at(params);
    yield* reopened.caughtUp;
  }).pipe(Effect.provide(inputHost)),
);

it.live("captures the startup tail once and catches up before a later write is read", () =>
  Effect.gen(function* () {
    yield* createShop;
    const initial = yield* putOrder(1);
    const shop = yield* Shop;
    const captured = yield* Deferred.make<void>();
    const readFirst = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let heads = 0;

    const live = yield* liveOrders.at(params).pipe(
      Effect.provideService(Shop, {
        ...shop,
        wire: {
          ...shop.wire,
          head: (id) =>
            shop.wire.head(id).pipe(
              Effect.tap(() => Effect.sync(() => heads++)),
              Effect.tap(() => Deferred.succeed(captured, undefined)),
            ),
          read: (id, options) =>
            options?.offset === ZERO_OFFSET
              ? shop.wire.read(id, options).pipe(
                  Effect.tap(() => Deferred.succeed(readFirst, undefined)),
                  Effect.tap(() => Deferred.await(release)),
                )
              : Effect.never,
        },
      }),
    );

    yield* Deferred.await(captured);
    yield* Deferred.await(readFirst);
    yield* putOrder(2);
    yield* Deferred.succeed(release, undefined);
    yield* live.caughtUp;
    const snapshot = yield* live.snapshot;
    expect(snapshot.offsets[Shop.key]).toBe(initial.offset);
    expect(snapshot.rows.orders[0]?.total).toBe(1);
    expect(heads).toBe(1);
  }).pipe(Effect.provide(inputHost)),
);

it.live("fails snapshots with a terminal startup HEAD fault", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(7);
    const shop = yield* Shop;
    const failHead = yield* Deferred.make<void>();
    const fault = new StorageFault({ operation: "head", message: "fatal head", retryable: false });

    const live = yield* liveOrders.at(params).pipe(
      Effect.provideService(Shop, {
        ...shop,
        wire: { ...shop.wire, head: () => Deferred.await(failHead).pipe(Effect.andThen(fault)) },
      }),
    );

    const replayed = yield* awaitMatch(live.snapshot, ({ rows }) => rows.orders.length > 0);
    expect(replayed.caughtUp).toBe(false);
    yield* Deferred.succeed(failHead, undefined);

    const error = yield* live.snapshots.pipe(Stream.runDrain, Effect.flip);
    expect(error).toMatchObject({ reason: "storage-failure", input: Shop.key, cause: fault });
    expect(yield* live.caughtUp.pipe(Effect.flip)).toBe(error);
    expect(yield* live.snapshot).toBe(replayed);
  }).pipe(Effect.provide(inputHost)),
);

it.live("fails snapshots when a finite retry schedule for HEAD runs out", () =>
  Effect.gen(function* () {
    yield* createShop;
    const shop = yield* Shop;
    let heads = 0;

    const offline = new StorageFault({
      operation: "head",
      message: "head unavailable",
      retryable: true,
    });

    const live = yield* View.live({
      id: "finite-retry",
      params: Shop.paramCodecs,
      views: { orders },
      retry: Schedule.recurs(1),
    })
      .at(params)
      .pipe(
        Effect.provideService(Shop, {
          ...shop,
          wire: {
            ...shop.wire,
            head: () =>
              Effect.suspend(() => {
                heads++;

                return offline;
              }),
          },
        }),
      );

    const error = yield* live.snapshots.pipe(Stream.runDrain, Effect.flip);
    expect(error).toMatchObject({ reason: "storage-failure", cause: offline, retryable: true });
    expect(yield* live.caughtUp.pipe(Effect.flip)).toBe(error);
    expect(heads).toBe(2);
  }).pipe(Effect.provide(inputHost)),
);
