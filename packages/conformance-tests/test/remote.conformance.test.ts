/** The official server suite exercises the remote Layer through a local HTTP gateway. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { MemoryStorage } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";

let backend: TestHost | undefined;

let gateway: TestHost | undefined;

describe("Official conformance through RemoteStreams", () => {
  const config = { baseUrl: "" };
  beforeAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        backend = yield* testHost({
          layer: MemoryStorage.layerProtocol({ longPollTimeoutMs: 1500 }),
          port: 0,
        });
        gateway = yield* testHost({
          layer: RemoteStreams.layerProtocol({
            baseUrl: new URL("/streams", backend.url).href,
            capabilities: { expectedOffset: true, producer: true },
          }).pipe(Layer.provide(FetchHttpClient.layer)),
          port: 0,
        });
        config.baseUrl = new URL(gateway.url).origin;
      }),
    ),
  );
  afterAll(() =>
    Effect.runPromise(
      (gateway?.stop ?? Effect.void).pipe(Effect.ensuring(backend?.stop ?? Effect.void)),
    ),
  );
  runConformanceTests(config);
});
