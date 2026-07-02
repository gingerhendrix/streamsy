/**
 * Bun entry that serves a SQLite-backed Streamsy HTTP handler.
 *
 * Run under the Bun runtime (it imports `bun:sqlite` transitively). The
 * conformance test spawns this as a child process because the official suite
 * runs under vitest/node, where `bun:sqlite` is unavailable.
 *
 * Reads `PORT` and `DB_PATH` from the environment. Prints `LISTENING <port>`
 * once ready so the parent can synchronize.
 */
import { StreamProtocol, HttpHandler } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

const port = Number(process.env.PORT ?? 0);
const filename = process.env.DB_PATH ?? ":memory:";

const adapter = createSqliteStorageAdapter({ filename });
const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1500 });
const handler = new HttpHandler({ protocol, pathPrefix: "/" });

const server = Bun.serve({
  port,
  fetch: (req: Request) => handler.fetch(req),
});

console.log(`LISTENING ${server.port}`);

const shutdown = () => {
  server.stop(true);
  adapter.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
