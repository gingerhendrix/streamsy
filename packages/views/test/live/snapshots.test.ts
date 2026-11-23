/* oxlint-disable effecttsgo/strict-effect-provide -- Each test runs its own page and instruments one input when needed. */
import { expect, it } from "@effect/vitest";
import { DurableState } from "@streamsy/core";
import { CheckpointStore, ProjectionFault } from "@streamsy/projection";
import { Array, Deferred, Effect, Fiber, Schedule, Schema, Stream } from "effect";
import { View } from "../../src/index.ts";
import { customers, orders, Shop } from "../circuit/support/shop.ts";
import {
  append,
  awaitMatch,
  createShop,
  liveHost,
  liveOrders,
  params,
  putOrder,
} from "./support.ts";

it.live("signals caughtUp once and keeps the rows array when only caughtUp changes", () =>
  Effect.gen(function* () {
    yield* createShop;
    const shop = yield* Shop;
    const letHeadRun = yield* Deferred.make<void>();
    const initial = yield* Deferred.make<void>();

    const live = yield* liveOrders.at(params).pipe(
      Effect.provideService(Shop, {
        ...shop,
        wire: {
          ...shop.wire,
          head: (id) => Deferred.await(letHeadRun).pipe(Effect.andThen(shop.wire.head(id))),
        },
      }),
    );

    const before = yield* live.rows("orders");

    const subscriber = yield* live.changes("orders").pipe(
      Stream.tap(() => Deferred.succeed(initial, undefined)),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkScoped,
    );

    yield* Deferred.await(initial);
    yield* Deferred.succeed(letHeadRun, undefined);
    yield* live.caughtUp;
    const snapshots = yield* Fiber.join(subscriber);
    expect(snapshots).toEqual([
      { rows: [], caughtUp: false },
      { rows: [], caughtUp: true },
    ]);
    expect(snapshots[0]?.rows).toBe(before.rows);
    expect(snapshots[1]?.rows).toBe(before.rows);
    expect(yield* live.rows("orders")).toBe(snapshots[1]);
  }).pipe(Effect.provide(liveHost)),
);

it.live("gives slow and late subscribers the latest committed snapshot", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(0);
    const live = yield* liveOrders.at(params);
    yield* live.caughtUp;
    const initial = yield* Deferred.make<void>();
    const pullAgain = yield* Deferred.make<void>();
    const first = yield* live.rows("orders");

    const slow = yield* live.changes("orders").pipe(
      Stream.tap((snapshot) =>
        snapshot === first
          ? Deferred.succeed(initial, undefined).pipe(Effect.andThen(Deferred.await(pullAgain)))
          : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkScoped,
    );

    yield* Deferred.await(initial);

    for (const total of [1, 2, 3]) {
      yield* putOrder(total);
      yield* awaitMatch(live.rows("orders"), ({ rows }) => rows[0]?.total === total);
    }

    const latest = yield* live.rows("orders");

    const [late] = yield* live.changes("orders").pipe(Stream.take(1), Stream.runCollect);
    expect(late).toBe(latest);

    yield* Deferred.succeed(pullAgain, undefined);
    const [initialSnapshot, resumed] = yield* Fiber.join(slow);
    expect(initialSnapshot).toBe(first);
    expect(resumed).toBe(latest);
    expect(resumed?.rows[0]?.total).toBe(3);
  }).pipe(Effect.provide(liveHost)),
);

it.live("orders encoded keys and protects nested rows and cached snapshots from mutation", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* append([
      DurableState.upsert("customer", { customerId: "c1", name: "Name", tier: "gold" }),
    ]);

    for (const key of ["\u{10000}", "\ufffd", "\udfff", "\ue000x", "\ud800", "a"])
      yield* putOrder(1, key);

    const joined = orders.join(customers, {
      as: "customer",
      on: (order, customer) => order.customerId.eq(customer.customerId),
    });

    const live = yield* View.live({
      id: "live-ordering",
      params: Shop.paramCodecs,
      views: { orders, joined },
    }).at(params);

    yield* live.caughtUp;
    const snapshot = yield* live.rows("orders");
    expect(Array.map(snapshot.rows, ({ orderId }) => orderId)).toEqual([
      "a",
      "\ud800",
      "\udfff",
      "\ufffd",
      "\ue000x",
      "\u{10000}",
    ]);

    const joinedSnapshot = yield* live.rows("joined");
    const first = joinedSnapshot.rows[0]!;
    expect(Reflect.set(joinedSnapshot, "caughtUp", false)).toBe(false);
    expect(Reflect.set(joinedSnapshot.rows, 0, undefined)).toBe(false);
    expect(Reflect.set(first.customer, "name", "corrupted")).toBe(false);

    yield* putOrder(2, "a");
    const updated = yield* awaitMatch(live.rows("joined"), ({ rows }) => rows[0]?.total === 2);
    expect(updated.rows[0]?.customer.name).toBe("Name");
    expect(joinedSnapshot.rows[0]?.total).toBe(1);
  }).pipe(Effect.provide(liveHost)),
);

it.live("ends normally when all inputs close and preserves rows until scope close", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(1);
    const shop = yield* Shop;
    yield* shop.append(params, [], { close: true });
    const live = yield* liveOrders.at(params);
    const snapshots = yield* live.changes("orders").pipe(Stream.runCollect);
    yield* live.caughtUp;

    expect(snapshots.at(-1)).toEqual({
      caughtUp: true,
      rows: [{ orderId: "o1", customerId: "c1", total: 1 }],
    });
    expect(yield* live.rows("orders")).toBe(snapshots.at(-1));
  }).pipe(Effect.provide(liveHost)),
);

it.live("reports missing input history and ends normally when a known input is removed", () =>
  Effect.gen(function* () {
    const missing = yield* Effect.scoped(
      Effect.gen(function* () {
        const live = yield* liveOrders.at(params);
        expect(yield* live.caughtUp.pipe(Effect.flip)).toMatchObject({
          reason: "history-unavailable",
          retryable: false,
        });
        expect(yield* live.rows("orders")).toEqual({ rows: [], caughtUp: false });

        return yield* live.changes("orders").pipe(Stream.runDrain, Effect.flip);
      }),
    );

    expect(missing).toMatchObject({ reason: "history-unavailable", retryable: false });
    yield* createShop;
    yield* putOrder(1);
    const live = yield* liveOrders.at(params);
    yield* live.caughtUp;
    const current = yield* live.rows("orders");
    const shop = yield* Shop;

    yield* shop.remove(params);
    expect(yield* live.changes("orders").pipe(Stream.runCollect)).toEqual([current]);
    expect(yield* live.rows("orders")).toBe(current);
  }).pipe(Effect.provide(liveHost)),
);

it.live("retries settlement and leaves an unchanged view's snapshot cached", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(3);
    const store = yield* CheckpointStore;
    let attempts = 0;

    const fault = new ProjectionFault({
      phase: "checkpoint",
      reason: "token-conflict",
      message: "retry one failed settlement",
    });

    const live = yield* View.live({
      id: "live-retry",
      params: { ...Shop.paramCodecs, minimum: Schema.FiniteFromString },
      views: ({ minimum }) => ({
        orders,
        filtered: orders.where((row) => row.total.gt(minimum)),
      }),
      retry: Schedule.recurs(1),
    })
      .at({ ...params, minimum: 2 })
      .pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (...args) =>
            ++attempts === 1 ? Effect.fail(fault) : store.compareAndSet(...args),
        }),
      );

    yield* live.caughtUp;
    expect(attempts).toBe(2);
    const before = yield* live.rows("filtered");

    const [o1] = before.rows;
    expect(o1?.total).toBe(3);

    yield* putOrder(1, "o2");
    yield* awaitMatch(live.rows("orders"), ({ rows }) => rows.length === 2);
    expect(yield* live.rows("filtered")).toBe(before);
  }).pipe(Effect.provide(liveHost)),
);
