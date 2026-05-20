/**
 * Memory-backed Durable Streams server (Bun)
 *
 * Demonstrates wiring HttpHandler + StreamProtocol + MemoryStreamStorage
 * on a Bun runtime. Used for running conformance tests.
 */

import { StreamProtocol, HttpHandler } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const store = createMemoryStreamStore();
const protocol = new StreamProtocol(store);
const handler = new HttpHandler({ protocol, pathPrefix: "/" });

const port = parseInt(process.env.PORT ?? "1337", 10);

const server = Bun.serve({
  port,
  fetch: (req) => handler.fetch(req),
});

console.log(`Memory server listening on http://localhost:${server.port}`);

export { server };
