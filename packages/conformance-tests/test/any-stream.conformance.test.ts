/** Run the unchanged official protocol suite through an explicit AnyStream family. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { AnyStream, Http, Memory } from "@streamsy/core";
import { listener } from "@streamsy/serve/bun";

describe("AnyStream memory Bun host", () => {
  const config = { baseUrl: "" };
  const runtime = ManagedRuntime.make(
    HttpRouter.serve(Http.routes([{ family: AnyStream, access: "full" }]), {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provide(Memory.layer({ longPollTimeoutMs: 1500 }, AnyStream)),
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
