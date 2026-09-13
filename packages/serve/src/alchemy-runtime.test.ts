import { expect, test } from "bun:test";
import { Effect, Layer } from "effect";
import { Memory, Protocol, StorageFault } from "@streamsy/core";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Alarm } from "./cloudflare/alarm.ts";
import { ObjectOptions } from "./cloudflare/object-options.ts";
import { Option } from "effect";
import { objectHandlers } from "../test/alchemy/runtime.ts";

test("Alchemy construction lazily retries acquisition, shares a successful build, and closes its scope", async () => {
  let acquisitions = 0;
  let releases = 0;
  let alarms = 0;
  const protocol = Protocol.layer().pipe(Layer.provideMerge(Memory.layer()));
  const layer = Layer.unwrap(
    Effect.gen(function* () {
      acquisitions += 1;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releases += 1;
        }),
      );
      if (acquisitions === 1)
        return yield* Effect.fail(
          new StorageFault({
            operation: "fixture.acquire",
            message: "first build fails",
            retryable: true,
          }),
        );
      return Layer.mergeAll(
        protocol,
        Layer.succeed(ObjectOptions, { pathPrefix: "/streams" }),
        Layer.succeed(Alarm, {
          current: Effect.succeed(Option.none()),
          arm: () => Effect.void,
          clear: Effect.sync(() => {
            alarms += 1;
          }),
        }),
      );
    }),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const handlers = yield* objectHandlers(layer);
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
      yield* handlers.alarm();
      expect(acquisitions).toBe(2);
      expect(releases).toBe(1);
      expect(alarms).toBe(1);
    }).pipe(Effect.scoped),
  );
  expect(releases).toBe(2);
});
