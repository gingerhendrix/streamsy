/**
 * Memory-backed Durable Streams server (Bun)
 *
 * Demonstrates wiring createHttpHandler + createStreamProtocol + memory storage
 * on a Bun runtime. Used for running conformance tests.
 */

import {
  createHttpHandler,
  createMemoryStorageAdapter,
  createStreamProtocol,
} from "@streamsy/core";

const factory = createMemoryStorageAdapter();
const protocol = createStreamProtocol({ storage: { adapter: factory } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

const port = parseInt(process.env.PORT ?? "1337", 10);

const server = Bun.serve({
  port,
  fetch: (req) => handler.fetch(req),
});

console.log(`Memory server listening on http://localhost:${server.port}`);

export { server };
