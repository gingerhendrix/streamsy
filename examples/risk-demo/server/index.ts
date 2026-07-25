/**
 * Bun HTTP entry for the durable Risk API.
 *
 * Opens one SQLite database (path from `DB_PATH`, default `:memory:`), backs the
 * Streamsy protocol with the SQLite storage adapter, and puts the capability /
 * game / command tables in the same database — so canonical events, the board
 * projection, and all metadata persist together and survive restart.
 *
 * Serves the JSON API plus the React board SPA (Bun bundles `index.html`).
 * Prints `LISTENING <port>` once serving (used by the smoke/restart harness).
 */

import { rmSync } from "node:fs";
import { createStreamProtocol } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

import { buildApp } from "./http/app.ts";
import { createSqliteStores } from "./persistence/sqlite-store.ts";
import index from "../public/index.html";

const port = Number.parseInt(process.env.PORT ?? "1339", 10);
const dbPath = process.env.DB_PATH ?? ":memory:";
const deleteDbOnExit = process.env.DELETE_DB_ON_EXIT === "1";

const adapter = createSqliteStorageAdapter({ filename: dbPath });
const protocol = createStreamProtocol({ storage: { adapter } });
// Metadata tables share the adapter's own database, so events, projection, and
// capability/command rows persist together (durable when DB_PATH is a file).
const stores = createSqliteStores(adapter.state.db);

const app = buildApp({ protocol, stores });

// Restart recovery: defence timers are derived from canonical pending state, not
// stored, so a process that comes back up rebuilds every outstanding timer — and
// resolves any whose deadline passed while it was down — before serving.
await app.defenseTimers.recover();

const isDevelopment = process.env.NODE_ENV !== "production";

const server = Bun.serve({
  port,
  idleTimeout: 60,
  routes: {
    // JSON API and the scoped, read-only board stream facade share the handler.
    "/v1/*": (request: Request) => app.fetch(request),
    "/streams/*": (request: Request) => app.fetch(request),
    "/openapi.json": (request: Request) => app.fetch(request),
    "/agent-seat/*": (request: Request) => app.fetch(request),
    "/agent/*": (request: Request) => app.fetch(request),
    "/healthz": () =>
      new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      }),
    // Everything else is the bundled React board (SPA fallback).
    "/*": index,
  },
  development: isDevelopment && { hmr: true },
  fetch: (request) => app.fetch(request),
});

const shutdown = (): void => {
  server.stop(true);
  app.defenseTimers.stop();
  adapter.close();
  if (deleteDbOnExit && dbPath !== ":memory:") {
    for (const path of [dbPath, `${dbPath}-shm`, `${dbPath}-wal`]) rmSync(path, { force: true });
  }
  process.exit(0);
};
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);

console.log(`LISTENING ${server.port}`);

export { server };
