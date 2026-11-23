/* oxlint-disable effecttsgo/strict-effect-provide -- Each page builds its own services at the test entry point. */
import { it } from "@effect/vitest";
import { DurableState, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Commit, Projection } from "@streamsy/projection";
import { Deferred, Effect, Layer, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { expect } from "vitest";
import { ViewBackend } from "../../src/index.ts";
import { Shop } from "../circuit/support/shop.ts";

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
