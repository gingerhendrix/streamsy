/* oxlint-disable effecttsgo/strict-effect-provide -- Each test owns the backend and may instrument one service. */
import { expect, it } from "@effect/vitest";
import { Streamsy } from "@streamsy/core";
import { attachOwner, ownerOf } from "@streamsy/core/internal";
import { CheckpointStore, ProjectionFault } from "@streamsy/projection";
import { Deferred, Effect, Exit, Fiber, Scope, Stream } from "effect";
import { View, ViewFault } from "../../src/index.ts";
import { memoryStateOf } from "../../src/memory/backend.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { orders, Shop } from "../circuit/support/shop.ts";
import { hostFor, sqlite } from "../support/backends.ts";
import {
  awaitMatch,
  checkpointKey,
  createShop,
  liveHost,
  liveOrders,
  params,
  putOrder,
} from "./support.ts";

const assertClean = Effect.gen(function* () {
  const checkpoints = yield* CheckpointStore;
  const backend = yield* currentBackend;
  expect(memoryStateOf(backend)?.size).toBe(0);
  expect((yield* checkpoints.load(checkpointKey)).token).toBe("0");
});

it.live("rejects SQL and duplicate readers while allowing different circuits on one backend", () =>
  Effect.gen(function* () {
    const sqliteHost = hostFor(sqlite, Streamsy.group(Shop));

    expect(
      yield* liveOrders.at(params).pipe(Effect.provide(sqliteHost), Effect.flip),
    ).toBeInstanceOf(ViewFault);

    yield* createShop;
    yield* putOrder(2);

    const another = View.live({
      id: "live-filtered",
      params: Shop.paramCodecs,
      views: { orders: orders.where((row) => row.total.lt(0)) },
    });

    const firstScope = yield* Scope.fork(yield* Effect.scope);
    const first = yield* liveOrders.at(params).pipe(Scope.provide(firstScope));
    const second = yield* another.at(params);

    expect(yield* liveOrders.at(params).pipe(Effect.flip)).toBeInstanceOf(ViewFault);
    yield* first.caughtUp;
    yield* second.caughtUp;
    expect((yield* first.rows("orders")).rows).toHaveLength(1);
    expect((yield* second.rows("orders")).rows).toHaveLength(0);

    yield* Scope.close(firstScope, Exit.void);
    const third = yield* liveOrders.at(params);
    yield* third.caughtUp;
    expect((yield* third.rows("orders")).rows).toHaveLength(1);
    expect((yield* second.rows("orders")).rows).toHaveLength(0);
  }).pipe(Effect.provide(liveHost)),
);

it.live(
  "returns committed rows while settlement is open, then fails readers with the follower",
  () =>
    Effect.gen(function* () {
      yield* createShop;
      yield* putOrder(1);
      const store = yield* CheckpointStore;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const reject = new ProjectionFault({
        phase: "checkpoint",
        reason: "invalid-record",
        message: "reject staged output",
      });

      let armed = false;
      const scope = yield* Scope.fork(yield* Effect.scope);

      const live = yield* liveOrders.at(params).pipe(
        Scope.provide(scope),
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (...args) =>
            armed
              ? Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(reject),
                )
              : store.compareAndSet(...args),
        }),
      );

      yield* live.caughtUp;
      const before = yield* live.rows("orders");
      const subscribed = yield* Deferred.make<void>();

      const changes = yield* live.changes("orders").pipe(
        Stream.runForEach((snapshot) =>
          Effect.sync(() => expect(snapshot).toBe(before)).pipe(
            Effect.andThen(Deferred.succeed(subscribed, undefined)),
          ),
        ),
        Effect.flip,
        Effect.forkScoped,
      );

      yield* Deferred.await(subscribed);
      armed = true;
      yield* putOrder(2);
      yield* Deferred.await(entered);
      expect(yield* live.rows("orders")).toBe(before);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(changes)).toBe(reject);
      expect(yield* live.caughtUp.pipe(Effect.flip)).toBe(reject);
      expect(yield* live.changes("orders").pipe(Stream.runHead, Effect.flip)).toBe(reject);
      expect(yield* live.rows("orders")).toBe(before);

      yield* Scope.close(scope, Exit.void);
      yield* assertClean;

      const replay = yield* liveOrders.at(params);
      yield* replay.caughtUp;
      expect((yield* replay.rows("orders")).rows[0]?.total).toBe(2);
    }).pipe(Effect.provide(liveHost)),
);

it.live("interrupts a unit and ends an outside subscription when its reader scope closes", () =>
  Effect.gen(function* () {
    yield* createShop;
    yield* putOrder(1);
    const store = yield* CheckpointStore;
    const entered = yield* Deferred.make<void>();
    const exited = yield* Deferred.make<void>();
    const scope = yield* Scope.fork(yield* Effect.scope);

    const live = yield* liveOrders.at(params).pipe(
      Scope.provide(scope),
      Effect.provideService(CheckpointStore, {
        ...store,
        compareAndSet: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(exited, undefined)),
          ),
      }),
    );

    yield* Deferred.await(entered);
    const subscribed = yield* Deferred.make<void>();

    const subscription = yield* live.changes("orders").pipe(
      Stream.tap(() => Deferred.succeed(subscribed, undefined)),
      Stream.runCollect,
      Effect.forkScoped,
    );

    yield* Deferred.await(subscribed);
    expect((yield* live.rows("orders")).rows).toEqual([]);
    yield* Scope.close(scope, Exit.void);
    expect(yield* Deferred.isDone(exited)).toBe(true);
    expect(yield* Fiber.join(subscription)).toEqual([{ rows: [], caughtUp: false }]);
    yield* assertClean;

    const replay = yield* liveOrders.at(params);
    yield* replay.caughtUp;
    expect((yield* replay.rows("orders")).rows).toHaveLength(1);
  }).pipe(Effect.provide(liveHost)),
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
    yield* assertClean;

    const replay = yield* liveOrders.at(params);
    yield* replay.caughtUp;
    expect((yield* replay.rows("orders")).rows).toEqual([]);
  }).pipe(Effect.provide(liveHost)),
);

it.live("leaves the caller scope reusable when acquisition itself is interrupted", () =>
  Effect.gen(function* () {
    yield* createShop;
    const scope = yield* Scope.fork(yield* Effect.scope);
    const store = yield* CheckpointStore;
    const started = yield* Deferred.make<void>();

    const load: typeof store.load = () =>
      Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));

    const acquire = yield* liveOrders.at(params).pipe(
      Scope.provide(scope),
      Effect.provideService(CheckpointStore, {
        ...store,
        load: attachOwner(load, ownerOf(store.load)),
      }),
      Effect.forkScoped,
    );

    yield* Deferred.await(started);
    yield* Fiber.interrupt(acquire);
    yield* assertClean;

    const live = yield* liveOrders.at(params).pipe(Scope.provide(scope));
    yield* awaitMatch(live.rows("orders"), ({ caughtUp }) => caughtUp);
    yield* Scope.close(scope, Exit.void);
    yield* assertClean;
  }).pipe(Effect.provide(liveHost)),
);
