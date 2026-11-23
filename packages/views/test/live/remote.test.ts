/* oxlint-disable effecttsgo/strict-effect-provide -- Each page builds its own services at the test entry point. */
import { it } from "@effect/vitest";
import { DurableState, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Commit, Projection, ProjectionFault } from "@streamsy/projection";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Queue,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { FetchHttpClient } from "effect/http";
import { expect } from "vitest";
import { View, ViewBackend, type LiveSnapshot } from "../../src/index.ts";
import { orders, Shop, type Order } from "../circuit/support/shop.ts";
import { awaitMatch } from "./support.ts";

const child = Effect.acquireRelease(
  Effect.sync(() =>
    Bun.spawn([process.execPath, `${import.meta.dirname}/remote-server.ts`], {
      stdout: "pipe",
      stderr: "inherit",
    }),
  ),
  (server) =>
    Effect.sync(() => server.kill()).pipe(Effect.andThen(Effect.promise(() => server.exited))),
);

const page = Effect.gen(function* () {
  const server = yield* child;
  const reader = server.stdout.getReader();
  const { value } = yield* Effect.promise(() => reader.read());
  const baseUrl = new TextDecoder().decode(value).trim();

  return Layer.merge(
    RemoteStreams.layer(Streamsy.group(Shop), { baseUrl }).pipe(
      Layer.provide(FetchHttpClient.layer),
    ),
    ViewBackend.layerMemoryHost,
  );
});

it.live("follows and catches up with a State stream on a real HTTP server", () =>
  Effect.gen(function* () {
    const host = yield* page;

    yield* Effect.gen(function* () {
      const params = { shop: "remote" };
      const shop = yield* Shop;
      yield* shop.create(params);
      const order = { orderId: "o1", customerId: "c1", total: 10 };

      const change = DurableState.changes(Shop.at(params), {}, [
        DurableState.upsert("order", order),
      ]);

      yield* shop.append(params, change);

      const seen = yield* Deferred.make();

      const binding = Projection.process({
        id: "remote-state-probe",
        params: Shop.paramCodecs,
        inputs: { shop: Shop },
        commit: Commit.ownerTransaction,
        process: ({ shop: slice }) => Deferred.succeed(seen, slice.items).pipe(Effect.asVoid),
      }).at(params);

      yield* Projection.follow(binding).pipe(Stream.runDrain, Effect.forkScoped);
      yield* Projection.caughtUp(binding).pipe(Effect.timeout("3 seconds"));
      expect(yield* Deferred.await(seen)).toEqual(change);
    }).pipe(Effect.provide(host));
  }).pipe(Effect.scoped),
);

it.live("replays and follows the same remote State from two independent page layers", () =>
  Effect.gen(function* () {
    const host = yield* page;
    const firstPage = yield* Layer.build(host);
    const secondPage = yield* Layer.build(host);
    const params = { shop: "two-pages" };
    const order = { orderId: "o1", customerId: "c1", total: 3 };

    const append = (total: number) =>
      Shop.use((shop) =>
        shop.append(
          params,
          DurableState.changes(Shop.at(params), {}, [
            DurableState.upsert("order", { ...order, total }),
          ]),
        ),
      ).pipe(Effect.provideContext(firstPage));

    yield* Shop.use((shop) => shop.create(params)).pipe(Effect.provideContext(firstPage));
    yield* append(3);

    const declaration = View.live({
      id: "remote-orders",
      params: Shop.paramCodecs,
      views: { orders },
    });

    const shop = Context.get(firstPage, Shop);
    let waiting = 0;

    const observed = Context.add(firstPage, Shop, {
      ...shop,
      wire: {
        ...shop.wire,
        readNext: (...args) =>
          Effect.acquireUseRelease(
            Effect.sync(() => {
              waiting++;
            }),
            () => shop.wire.readNext(...args),
            () =>
              Effect.sync(() => {
                waiting--;
              }),
          ),
      },
    });

    const scope = yield* Scope.fork(yield* Effect.scope);

    const first = yield* declaration
      .at(params)
      .pipe(Scope.provide(scope), Effect.provideContext(observed));

    const second = yield* declaration.at(params).pipe(Effect.provideContext(secondPage));

    yield* Effect.all([first.caughtUp, second.caughtUp], { concurrency: "unbounded" });
    expect(yield* first.rows("orders")).toEqual({ rows: [order], caughtUp: true });
    expect(yield* second.rows("orders")).toEqual({ rows: [order], caughtUp: true });

    yield* append(4);
    yield* awaitMatch(first.rows("orders"), ({ rows }) => rows[0]?.total === 4);
    yield* awaitMatch(second.rows("orders"), ({ rows }) => rows[0]?.total === 4);

    yield* awaitMatch(
      Effect.sync(() => waiting),
      (active) => active > 0,
    );
    yield* Scope.close(scope, Exit.void);
    expect(waiting).toBe(0);
    yield* append(5);
    expect(
      (yield* awaitMatch(second.rows("orders"), ({ rows }) => rows[0]?.total === 5)).caughtUp,
    ).toBe(true);
  }).pipe(Effect.scoped),
);

it.live("recovers from refused HTTP heads and reads, and signals caughtUp once", () =>
  Effect.gen(function* () {
    const host = yield* page;

    yield* Effect.gen(function* () {
      const params = { shop: "recovering" };
      const shop = yield* Shop;
      const order = { orderId: "o1", customerId: "c1", total: 10 };

      yield* shop.create(params);
      yield* shop.append(
        params,
        DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", order)]),
      );

      const retryHead = yield* Deferred.make<void>();
      const retryRead = yield* Deferred.make<void>();
      const refusedHead = yield* Deferred.make<void>();
      const refusedRead = yield* Deferred.make<void>();

      const retry = Schedule.spaced("10 millis").pipe(
        Schedule.tap(({ input }) => {
          if (!Schema.is(ProjectionFault)(input)) return Effect.void;

          return input.message.includes("the head")
            ? Deferred.succeed(refusedHead, undefined).pipe(
                Effect.andThen(Deferred.await(retryHead)),
              )
            : Deferred.succeed(refusedRead, undefined).pipe(
                Effect.andThen(Deferred.await(retryRead)),
              );
        }),
      );

      const scope = yield* Scope.fork(yield* Effect.scope);

      const reader = yield* View.live({
        id: "recovering",
        params: Shop.paramCodecs,
        views: { orders },
        retry,
      })
        .at(params)
        .pipe(Scope.provide(scope));

      const updates = yield* Queue.unbounded<LiveSnapshot<typeof Order.Type>>();

      const subscriber = yield* reader.changes("orders").pipe(
        Stream.tap((snapshot) => Queue.offer(updates, snapshot)),
        Stream.runCollect,
        Effect.forkScoped,
      );

      const initial = yield* Queue.take(updates);
      expect(initial).toEqual({ rows: [], caughtUp: false });
      yield* Deferred.succeed(retryRead, undefined);
      const replayed = yield* Queue.take(updates);
      expect(replayed).toEqual({ rows: [order], caughtUp: false });
      expect(yield* reader.rows("orders")).toBe(replayed);

      yield* Deferred.succeed(retryHead, undefined);
      yield* reader.caughtUp;
      expect(yield* Deferred.isDone(refusedHead)).toBe(true);
      expect(yield* Deferred.isDone(refusedRead)).toBe(true);
      const ready = yield* Queue.take(updates);
      expect(ready).toEqual({ rows: [order], caughtUp: true });
      expect(ready.rows).toBe(replayed.rows);

      yield* Scope.close(scope, Exit.void);
      expect(yield* Fiber.join(subscriber)).toEqual([initial, replayed, ready]);
    }).pipe(Effect.provide(host));
  }).pipe(Effect.scoped),
);
