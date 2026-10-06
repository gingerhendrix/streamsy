import { routes as protocolRoutes } from "@streamsy/core/protocol";
import { expect, it as baseIt } from "@streamsy/effect-test";
import { Effect, Layer } from "effect";
import { StorageFault, MemoryStorage } from "@streamsy/core";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { Alarm } from "../src/cloudflare/alarm.ts";
import { objectHandlers } from "../src/alchemy.ts";

baseIt.effect.each(["typed", "defect"])(
  "Alchemy retries %s acquisition failures and closes its scope",
  (failure) =>
    Effect.gen(function* () {
      let acquisitions = 0;
      let releases = 0;
      let alarms = 0;
      const protocol = MemoryStorage.layerProtocol();

      const layer = Layer.unwrap(
        Effect.gen(function* () {
          acquisitions += 1;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              releases += 1;
            }),
          );

          if (acquisitions === 1 && failure === "defect")
            return yield* Effect.die(new Error("fixture acquisition defect"));

          if (acquisitions === 1)
            return yield* new StorageFault({
              operation: "fixture.acquire",
              message: "first build fails",
              retryable: true,
            });

          return Layer.mergeAll(
            protocol,
            Layer.succeed(Alarm, {
              current: Effect.succeedNone,
              arm: () => Effect.void,
              clear: Effect.sync(() => {
                alarms += 1;
              }),
            }),
          );
        }),
      );

      yield* Effect.gen(function* () {
        const handlers = yield* objectHandlers({
          app: protocolRoutes({ prefix: "/streams" }),
          layer,
        });

        expect(acquisitions).toBe(0);

        const fetch = handlers.fetch.pipe(
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(
              new Request("https://streams.test/streams/s", { method: "OPTIONS" }),
            ),
          ),
        );

        const failed = HttpServerResponse.toWeb(yield* fetch);
        expect(failed.status).toBe(503);
        expect(failed.headers.get("retry-after")).toBe("1");
        expect(failed.headers.get("x-content-type-options")).toBe("nosniff");
        expect(yield* Effect.promise(() => failed.text())).toBe("Storage unavailable");
        expect(releases).toBe(1);
        const responses = yield* Effect.all([fetch, fetch], { concurrency: "unbounded" });
        expect(responses.map((response) => response.status)).toEqual([204, 204]);

        const missing = yield* handlers.fetch.pipe(
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(new Request("https://streams.test/outside")),
          ),
        );

        expect(missing.status).toBe(404);

        yield* handlers.alarm();
        expect(acquisitions).toBe(2);
        expect(releases).toBe(1);
        expect(alarms).toBe(1);
      }).pipe(Effect.scoped);
      expect(releases).toBe(2);
    }),
);
