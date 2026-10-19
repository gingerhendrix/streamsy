/**
 * Runs the official Durable Streams server suite against a deployed server.
 *
 * `e2e/` deploys the conformance Worker to Cloudflare, sets SERVER_BASE_URL and
 * runs this file through `bun run test:deployed`. It then destroys the Worker.
 */
import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { describe } from "vitest";

const baseUrl = Bun.env.SERVER_BASE_URL;

if (baseUrl === undefined || baseUrl.length === 0) {
  throw new Error("SERVER_BASE_URL must name the deployed server; run it from e2e/");
}

describe("Deployed Cloudflare Durable Object host", () => {
  runConformanceTests({ baseUrl: baseUrl.replace(/\/$/, "") });
});
