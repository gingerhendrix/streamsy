/** The official server suite exercises the memory protocol through a local Bun host. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect } from "effect";
import { Streams } from "@streamsy/core";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";

let host: TestHost | undefined;
describe("Effect memory Bun host", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    host = await Effect.runPromise(
      testHost({ layer: Streams.layerMemory({ longPollTimeoutMs: 1500 }), port: 0 }),
    );
    config.baseUrl = new URL(host.url).origin;
  });
  afterAll(async () => {
    if (host !== undefined) await Effect.runPromise(host.stop);
  });
  runConformanceTests(config);
});
