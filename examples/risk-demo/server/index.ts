/**
 * Bun HTTP entry for the durable Risk API.
 *
 * Opens one SQLite database (path from `DB_PATH`, default `:memory:`), backs the
 * Streamsy protocol with the SQLite storage adapter, and puts the capability /
 * game / command tables in the same database — so canonical events, the board
 * projection, and all metadata persist together and survive restart.
 *
 * Prints `LISTENING <port>` once serving (used by the smoke/restart harness).
 */

import { createStreamProtocol } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

import { buildApp } from "./app.ts";
import { createSqliteStores } from "./sqlite-store.ts";

const port = Number.parseInt(process.env.PORT ?? "1339", 10);
const dbPath = process.env.DB_PATH ?? ":memory:";

const adapter = createSqliteStorageAdapter({ filename: dbPath });
const protocol = createStreamProtocol({ storage: { adapter } });
// Metadata tables share the adapter's own database, so events, projection, and
// capability/command rows persist together (durable when DB_PATH is a file).
const stores = createSqliteStores(adapter.state.db);

const app = buildApp({ protocol, stores });

const server = Bun.serve({
  port,
  idleTimeout: 60,
  fetch: (request) => app.fetch(request),
});

const shutdown = (): void => {
  server.stop(true);
  adapter.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

console.log(`LISTENING ${server.port}`);

export { server };
