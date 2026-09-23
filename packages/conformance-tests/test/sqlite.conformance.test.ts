/** The official server suite exercises the SQLite protocol through a local Bun host. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import { Effect } from "effect";
import { testHost, type TestHost } from "../../serve/test/support/scoped-host.ts";
import * as SqlStorage from "@streamsy/storage/bun";

let host: TestHost | undefined;
describe("Effect SQLite Bun host", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    const scratch = process.env.STREAMSY_STORAGE_SCRATCH ?? "/tmp";
    const filename = `${scratch}/official-sqlite-conformance-${process.pid}-${crypto.randomUUID()}.sqlite`;
    host = await Effect.runPromise(
      testHost({
        layer: SqlStorage.layerProtocol({
          client: { filename },
          longPollTimeoutMs: 1_500,
        }),
        port: 0,
      }),
    );
    config.baseUrl = new URL(host.url).origin;
  });
  afterAll(async () => {
    if (host !== undefined) await Effect.runPromise(host.stop);
  });
  runConformanceTests(config);
});
