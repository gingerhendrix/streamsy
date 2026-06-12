/**
 * Memory-backed Durable Streams server (Bun)
 *
 * Demonstrates wiring createHttpHandler + createStreamProtocol + memory storage
 * on a Bun runtime. Used for running conformance tests.
 */

import { createHttpHandler, createMemoryStreamFactory, createStreamProtocol } from "@streamsy/core";

const factory = createMemoryStreamFactory();
const protocol = createStreamProtocol({ storage: { factory } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

const port = parseInt(process.env.PORT ?? "1337", 10);

const server = Bun.serve({
  port,
  fetch: (req) => handler.fetch(req),
});

console.log(`Memory server listening on http://localhost:${server.port}`);

export { server };
