/* oxlint-disable effecttsgo/strict-effect-provide -- Each test runs its own page and instruments one input when needed. */
import { expect, it } from "@effect/vitest";
import { DurableState, MemoryStorage, StorageFault, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Array, Deferred, Effect, Exit, Fiber, Schedule, Schema, Scope, Stream } from "effect";
import { View } from "../../src/index.ts";
import { customerTotals, customers, orders, Shop } from "../circuit/support/shop.ts";
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

    const before = yield* live.snapshot;

    const subscriber = yield* live.snapshots.pipe(
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
      { rows: { orders: [] }, offsets: { [Shop.key]: ZERO_OFFSET }, caughtUp: false },
      { rows: { orders: [] }, offsets: { [Shop.key]: ZERO_OFFSET }, caughtUp: true },
    ]);
    expect(snapshots[0]?.rows.orders).toBe(before.rows.orders);
    expect(snapshots[1]?.rows.orders).toBe(before.rows.orders);
    expect(yield* live.snapshot).toBe(snapshots[1]);
  }).pipe(Effect.provide(liveHost)),
);

it.live("gives slow and late subscribers consistent views at the latest committed offset", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(0);

    const live = yield* View.live({
      id: "live-consistent-views",
      params: Shop.paramCodecs,
      views: { orders, customerTotals },
    }).at(params);

    yield* live.caughtUp;

    const initial = yield* Deferred.make<void>();
    const pullAgain = yield* Deferred.make<void>();
    const first = yield* live.snapshot;

    const slow = yield* live.snapshots.pipe(
      Stream.tap(({ rows }) =>
        Effect.sync(() => {
          expect(rows.orders[0]?.total).toBe(rows.customerTotals[0]?.revenue);
        }),
      ),
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
      const { offset } = yield* putOrder(total);

      const snapshot = yield* awaitMatch(
        live.snapshot,
        ({ offsets }) => offsets[Shop.key] === offset,
      );

      expect(snapshot.rows.orders[0]?.total).toBe(total);
      expect(snapshot.rows.customerTotals[0]?.revenue).toBe(total);
    }

    const latest = yield* live.snapshot;
    const [late] = yield* live.snapshots.pipe(Stream.take(1), Stream.runCollect);
    expect(late).toBe(latest);

    yield* Deferred.succeed(pullAgain, undefined);
    const [initialSnapshot, resumed] = yield* Fiber.join(slow);
    expect(initialSnapshot).toBe(first);
    expect(resumed).toBe(latest);
    expect(resumed?.rows.orders[0]?.total).toBe(3);
  }).pipe(Effect.provide(liveHost)),
);

class Catalog extends Streamsy.state<Catalog>()("ViewsTest/Catalog", "catalog/:shop", {
  params: Shop.paramCodecs,
  collections: { customer: Shop.collections.customer },
}) {}

it.live("keeps a separate committed offset for each input", () =>
  Effect.gen(function* () {
    yield* createShop;
    const catalog = yield* Catalog;
    yield* catalog.create(params);
    const order = yield* putOrder(1);

    const live = yield* View.live({
      id: "live-input-offsets",
      params: Shop.paramCodecs,
      views: { orders, customers: View.from(Catalog, "customer") },
    }).at(params);

    yield* live.caughtUp;
    const before = yield* live.snapshot;
    expect(before.offsets).toEqual({ [Shop.key]: order.offset, [Catalog.key]: ZERO_OFFSET });

    const customer = { customerId: "c1", name: "Name", tier: "gold" };

    const { offset } = yield* catalog.append(
      params,
      DurableState.changes(Catalog.at(params), {}, [DurableState.upsert("customer", customer)]),
    );

    const after = yield* awaitMatch(
      live.snapshot,
      ({ offsets }) => offsets[Catalog.key] === offset,
    );

    expect(after.offsets).toEqual({ [Shop.key]: order.offset, [Catalog.key]: offset });
    expect(after.rows.customers).toEqual([customer]);
    expect(after.rows.orders).toBe(before.rows.orders);
  }).pipe(Effect.provide(MemoryStorage.layer(Streamsy.group(Shop, Catalog)))),
);

it.live("orders encoded keys and protects nested rows, offsets and snapshots from mutation", () =>
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
    const snapshot = yield* live.snapshot;
    expect(Array.map(snapshot.rows.orders, ({ orderId }) => orderId)).toEqual([
      "a",
      "\ud800",
      "\udfff",
      "\ufffd",
      "\ue000x",
      "\u{10000}",
    ]);

    const first = snapshot.rows.joined[0]!;
    expect(Reflect.set(snapshot, "caughtUp", false)).toBe(false);
    expect(Reflect.set(snapshot.rows, "joined", [])).toBe(false);
    expect(Reflect.set(snapshot.rows.joined, 0, undefined)).toBe(false);
    expect(Reflect.set(snapshot.offsets, Shop.key, ZERO_OFFSET)).toBe(false);
    expect(Reflect.set(first.customer, "name", "corrupted")).toBe(false);

    yield* putOrder(2, "a");
    const updated = yield* awaitMatch(live.snapshot, ({ rows }) => rows.joined[0]?.total === 2);
    expect(updated.rows.joined[0]?.customer.name).toBe("Name");
    expect(snapshot.rows.joined[0]?.total).toBe(1);
  }).pipe(Effect.provide(liveHost)),
);

it.live("ends normally when all inputs close and preserves the final rows after scope close", () =>
  Effect.gen(function* () {
    yield* createShop;
    const { offset } = yield* putOrder(1);
    const shop = yield* Shop;
    yield* shop.append(params, [], { close: true });

    const scope = yield* Scope.fork(yield* Effect.scope);
    const live = yield* liveOrders.at(params).pipe(Scope.provide(scope));
    const snapshots = yield* live.snapshots.pipe(Stream.runCollect);
    yield* live.caughtUp;

    expect(snapshots.at(-1)).toEqual({
      caughtUp: true,
      offsets: { [Shop.key]: offset },
      rows: { orders: [{ orderId: "o1", customerId: "c1", total: 1 }] },
    });

    yield* Scope.close(scope, Exit.void);
    expect(yield* live.snapshot).toBe(snapshots.at(-1));
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
        expect(yield* live.snapshot).toEqual({
          rows: { orders: [] },
          offsets: { [Shop.key]: ZERO_OFFSET },
          caughtUp: false,
        });

        return yield* live.snapshots.pipe(Stream.runDrain, Effect.flip);
      }),
    );

    expect(missing).toMatchObject({ reason: "history-unavailable", retryable: false });
    yield* createShop;
    const { offset } = yield* putOrder(1);
    const live = yield* liveOrders.at(params);
    yield* live.caughtUp;
    const shop = yield* Shop;

    yield* shop.remove(params);
    const snapshots = yield* live.snapshots.pipe(Stream.runCollect);
    const last = yield* live.snapshot;
    expect(snapshots.at(-1)).toBe(last);
    expect(last.offsets[Shop.key]).toBe(offset);
    expect(last.rows.orders[0]?.total).toBe(1);
  }).pipe(Effect.provide(liveHost)),
);

it.live("retries a read and keeps an unchanged view's row array when the offset advances", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(3);
    const shop = yield* Shop;
    let attempts = 0;

    const fault = new StorageFault({
      operation: "read",
      message: "retry one read",
      retryable: true,
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
        Effect.provideService(Shop, {
          ...shop,
          wire: {
            ...shop.wire,
            read: (...args) =>
              Effect.suspend(() => (++attempts === 1 ? fault : shop.wire.read(...args))),
          },
        }),
      );

    yield* live.caughtUp;
    expect(attempts).toBe(2);
    const before = yield* live.snapshot;
    expect(before.rows.filtered[0]?.total).toBe(3);

    const { offset } = yield* putOrder(1, "o2");
    const latest = yield* awaitMatch(live.snapshot, ({ offsets }) => offsets[Shop.key] === offset);
    expect(latest.rows.orders).toHaveLength(2);
    expect(latest.rows.filtered).toBe(before.rows.filtered);
  }).pipe(Effect.provide(liveHost)),
);

it.live(
  "delivers the latest snapshot and then the terminal fault even with a full subscriber buffer",
  () =>
    Effect.gen(function* () {
      yield* createShop;
      yield* putOrder(1);
      const shop = yield* Shop;
      const first = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const delivered: number[] = [];
      let reject = false;

      const fault = new StorageFault({
        operation: "read",
        message: "fail after the last successful snapshot",
        retryable: false,
      });

      const live = yield* liveOrders.at(params).pipe(
        Effect.provideService(Shop, {
          ...shop,
          wire: {
            ...shop.wire,
            read: (...args) => Effect.suspend(() => (reject ? fault : shop.wire.read(...args))),
          },
        }),
      );

      yield* live.caughtUp;

      const slow = yield* live.snapshots.pipe(
        Stream.runForEach(({ rows }) =>
          Effect.sync(() => delivered.push(rows.orders[0]!.total)).pipe(
            Effect.andThen(Deferred.succeed(first, undefined)),
            Effect.andThen(Deferred.await(resume)),
          ),
        ),
        Effect.flip,
        Effect.forkScoped,
      );

      yield* Deferred.await(first);

      for (const total of [2, 3, 4]) {
        yield* putOrder(total);
        yield* awaitMatch(live.snapshot, ({ rows }) => rows.orders[0]?.total === total);
      }

      const last = yield* live.snapshot;
      reject = true;
      yield* putOrder(5);
      const late: (typeof last)[] = [];

      const error = yield* live.snapshots.pipe(
        Stream.tap((snapshot) => Effect.sync(() => late.push(snapshot))),
        Stream.runDrain,
        Effect.flip,
      );

      expect(error).toMatchObject({ reason: "storage-failure", cause: fault });
      expect(late).toEqual([last]);

      yield* Deferred.succeed(resume, undefined);
      expect(yield* Fiber.join(slow)).toBe(error);
      expect(delivered).toEqual([1, 4]);
      expect(yield* live.snapshot).toBe(last);
    }).pipe(Effect.provide(liveHost)),
);
