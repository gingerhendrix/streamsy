/** Run the unchanged official protocol suite through an explicit Streamsy.Wildcard stream. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { Streamsy, Http, MemoryStorage } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";

describe("Streamsy.Wildcard memory Bun host", () => {
  const config = { baseUrl: "" };

  const runtime = ManagedRuntime.make(
    HttpRouter.serve(Http.routes(Streamsy.group(Streamsy.Wildcard), { access: "full" }), {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provide(
        MemoryStorage.layer(Streamsy.group(Streamsy.Wildcard), { longPollTimeoutMs: 1500 }),
      ),
      Layer.provideMerge(listener({ port: 0 })),
    ),
  );

  beforeAll(() =>
    runtime.runPromise(
      Effect.map(HttpServer.HttpServer, (server) => {
        config.baseUrl = HttpServer.formatAddress(server.address);
      }),
    ),
  );
  afterAll(() => runtime.dispose());
  runConformanceTests(config);
});
