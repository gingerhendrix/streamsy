/** The official server suite exercises the Fetch Layer through a local HTTP gateway. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { Streams } from "@streamsy/core";
import * as Fetch from "@streamsy/core/fetch";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";

let backend: TestHost | undefined;
let gateway: TestHost | undefined;
describe("Official conformance through Effect Fetch", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    backend = await Effect.runPromise(
      testHost({ layer: Streams.layerMemory({ longPollTimeoutMs: 1500 }), port: 0 }),
    );
    gateway = await Effect.runPromise(
      testHost({
        layer: Fetch.layer({
          baseUrl: new URL("/streams", backend.url).href,
          capabilities: { expectedOffset: true, producer: true },
        }).pipe(Layer.provide(FetchHttpClient.layer)),
        port: 0,
      }),
    );
    config.baseUrl = new URL(gateway.url).origin;
  });
  afterAll(async () => {
    try {
      if (gateway !== undefined) await Effect.runPromise(gateway.stop);
    } finally {
      if (backend !== undefined) await Effect.runPromise(backend.stop);
    }
  });
  runConformanceTests(config);
});
