/** The approved Vitest exception only registers the bundled official suite. */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";
import * as BunHost from "@streamsy/serve/bun";
import * as SqlStorage from "@streamsy/storage/bun";

let host: Awaited<ReturnType<typeof BunHost.serve>> | undefined;
describe("Effect SQLite Bun host", () => {
  const config = { baseUrl: "" };
  beforeAll(async () => {
    const scratch = process.env.STREAMSY_STORAGE_SCRATCH ?? "/tmp";
    const filename = `${scratch}/official-sqlite-conformance-${process.pid}-${crypto.randomUUID()}.sqlite`;
    host = await BunHost.serve({
      layer: SqlStorage.layerProtocol({
        client: { filename },
        longPollTimeoutMs: 1_500,
      }),
      port: 0,
    });
    config.baseUrl = host.url.origin;
  });
  afterAll(async () => {
    await host?.stop();
  });
  runConformanceTests(config);
});
