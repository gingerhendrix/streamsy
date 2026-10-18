/** The official server suite exercises the SQLite protocol through a local Bun host. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { NodeCrypto } from "@effect/platform-node";
import { Context, Crypto, Effect, Layer } from "effect";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";
import { BunStorage } from "@streamsy/storage/bun";

let host: TestHost | undefined;

describe("Effect SQLite Bun host", () => {
  const config = { baseUrl: "" };
  const scratch = process.env.STREAMSY_STORAGE_SCRATCH ?? "/tmp";
  beforeAll(() =>
    Effect.runPromise(
      Effect.gen(function* () {
        const crypto = Context.get(yield* Layer.build(NodeCrypto.layer), Crypto.Crypto);
        const id = yield* crypto.randomUUIDv4();
        const filename = `${scratch}/official-sqlite-conformance-${process.pid}-${id}.sqlite`;
        host = yield* testHost({
          layer: BunStorage.layerProtocol({
            client: { filename },
            longPollTimeoutMs: 1_500,
          }),
          port: 0,
        });
        config.baseUrl = new URL(host.url).origin;
      }).pipe(Effect.scoped),
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
