/* oxlint-disable effecttsgo/global-console -- This Bun executable is the Promise-native HTTP/process edge. */
import { Streams } from "@streamsy/core";
import * as Http from "@streamsy/core/http";

const port = parseInt(process.env.PORT ?? "1337", 10);
const edge = Http.makeEdge({ pathPrefix: "/" }, Streams.layerMemory());

const server = Bun.serve({
  port,
  idleTimeout: 60,
  fetch: (request) => edge.handler(request),
});

console.log(`Memory server listening on http://localhost:${server.port}`);

let shuttingDown: Promise<void> | undefined;
function shutdown(): Promise<void> {
  if (shuttingDown) return shuttingDown;
  const pending = edge.dispose().then(() => server.stop(true));
  shuttingDown = pending;
  return pending;
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => void shutdown().finally(() => process.exit(0)));
}

export { server, shutdown };
