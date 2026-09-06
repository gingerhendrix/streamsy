/** The approved Vitest exception only registers the bundled official suite. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Streams } from "@streamsy/core-next";
import * as BunHost from "@streamsy/serve/bun";

let host: Awaited<ReturnType<typeof BunHost.serve>> | undefined;
describe("Effect memory Bun host", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    host = await BunHost.serve({
      layer: Streams.layerMemory({ longPollTimeoutMs: 1500 }),
      port: 0,
    });
    config.baseUrl = host.url.origin;
  });
  afterAll(async () => {
    await host?.stop();
  });
  runConformanceTests(config);
});
