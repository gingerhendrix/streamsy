/** Run the unchanged official protocol suite through an explicit Streamsy.Wildcard stream. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { Streamsy, Http, Memory } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";

describe("Streamsy.Wildcard memory Bun host", () => {
  const config = { baseUrl: "" };
  const runtime = ManagedRuntime.make(
    HttpRouter.serve(Http.routes(Streamsy.group(Streamsy.Wildcard), { access: "full" }), {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provide(Memory.layer({ longPollTimeoutMs: 1500 }, Streamsy.group(Streamsy.Wildcard))),
      Layer.provideMerge(listener({ port: 0 })),
    ),
  );
  beforeAll(async () => {
    const server = await runtime.runPromise(HttpServer.HttpServer);
    config.baseUrl = HttpServer.formatAddress(server.address);
  });
  afterAll(() => runtime.dispose());
  runConformanceTests(config);
});
