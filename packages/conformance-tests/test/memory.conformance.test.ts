/** The official server suite exercises the memory protocol through a local Bun host. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";

let host: TestHost | undefined;

describe("Effect memory Bun host", () => {
  const config = { baseUrl: "" };
  beforeAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        host = yield* testHost({
          layer: MemoryStorage.layerProtocol({ longPollTimeoutMs: 1500 }),
          port: 0,
        });
        config.baseUrl = new URL(host.url).origin;
      }),
    ),
  );
  afterAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        if (host !== undefined) yield* host.stop;
      }),
    ),
  );
  runConformanceTests(config);
});
