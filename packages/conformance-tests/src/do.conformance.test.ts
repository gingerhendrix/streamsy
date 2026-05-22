/**
 * Run conformance tests against server implementations
 */

import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { describe } from "vitest";

describe(`Durable Object Server Implementation`, () => {
  const config = {
    baseUrl: process.env.SERVER_BASE_URL || "http://localhost:1337",
  };

  runConformanceTests(config);
});
