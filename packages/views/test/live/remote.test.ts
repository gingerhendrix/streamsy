/* oxlint-disable effecttsgo/strict-effect-provide -- Each page builds its own services at the test entry point. */
import { it } from "@effect/vitest";
import { DurableState, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Commit, Projection } from "@streamsy/projection";
import { Deferred, Effect, Exit, Layer, Scope, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { expect } from "vitest";
import { View, ViewBackend } from "../../src/index.ts";
import { orders, Shop } from "../circuit/support/shop.ts";
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

    const scope = yield* Scope.fork(yield* Effect.scope);

    const first = yield* declaration
      .at(params)
      .pipe(Scope.provide(scope), Effect.provideContext(firstPage));

    const second = yield* declaration.at(params).pipe(Effect.provideContext(secondPage));

    yield* Effect.all([first.caughtUp, second.caughtUp], { concurrency: "unbounded" });
    expect(yield* first.rows("orders")).toEqual({ rows: [order], caughtUp: true });
    expect(yield* second.rows("orders")).toEqual({ rows: [order], caughtUp: true });

    yield* append(4);
    yield* awaitMatch(first.rows("orders"), ({ rows }) => rows[0]?.total === 4);
    yield* awaitMatch(second.rows("orders"), ({ rows }) => rows[0]?.total === 4);

    yield* Scope.close(scope, Exit.void);
    yield* append(5);
    expect(
      (yield* awaitMatch(second.rows("orders"), ({ rows }) => rows[0]?.total === 5)).caughtUp,
    ).toBe(true);
  }).pipe(Effect.scoped),
);
