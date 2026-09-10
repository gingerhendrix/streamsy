/** The official server suite exercises the Fetch Layer through a local HTTP gateway. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { Streams } from "@streamsy/core";
import * as Fetch from "@streamsy/core/fetch";
import * as BunHost from "@streamsy/serve/bun";

let backend: Awaited<ReturnType<typeof BunHost.serve>> | undefined;
let gateway: Awaited<ReturnType<typeof BunHost.serve>> | undefined;
describe("Official conformance through Effect Fetch", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    backend = await BunHost.serve({
      layer: Streams.layerMemory({ longPollTimeoutMs: 1500 }),
      port: 0,
    });
    gateway = await BunHost.serve({
      layer: Fetch.layer({
        baseUrl: new URL("/streams", backend.url).href,
        capabilities: { expectedOffset: true, producer: true },
      }).pipe(Layer.provide(FetchHttpClient.layer)),
      port: 0,
    });
    config.baseUrl = gateway.url.origin;
  });
  afterAll(async () => {
    try {
      await gateway?.stop();
    } finally {
      await backend?.stop();
    }
  });
  runConformanceTests(config);
});
