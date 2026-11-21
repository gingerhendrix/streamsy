/* oxlint-disable effecttsgo/async-function -- This Bun executable is the Promise-native HTTP/process edge; reusable game and projection work remains owned by the host runtime. */
/* oxlint-disable effecttsgo/global-console, effecttsgo/node-builtin-import, effecttsgo/process-env -- This Bun executable directly owns terminal output, static-file loading, and process configuration around the host runtime. */
/**
 * Bun HTTP entry for the durable Risk API.
 *
 * With a file-backed DB_PATH, BunStorage and the metadata connection share one
 * SQLite file. Without DB_PATH, both stores are disposable.
 *
 * Serves the JSON API plus the React board SPA (Bun bundles `index.html`).
 * Prints `LISTENING <port>` once serving (used by the smoke/restart harness).
 */

import { rmSync } from "node:fs";
import { buildApp } from "./http/app.ts";
import { openBunStorage } from "./persistence/bun.ts";
import index from "../public/index.html";

const port = Number.parseInt(process.env.PORT ?? "1339", 10);
const dbPath = process.env.DB_PATH ?? ":memory:";
const deleteDbOnExit = process.env.DELETE_DB_ON_EXIT === "1";

const storage = await openBunStorage(dbPath);
const app = buildApp(storage);

// Restart recovery: defence timers are derived from canonical pending state, not
// stored, so a process that comes back up rebuilds every outstanding timer — and
// resolves any whose deadline passed while it was down — before serving.
await app.defenseTimers.recover();

const isDevelopment = process.env.NODE_ENV !== "production";

const server = Bun.serve({
  port,
  hostname: "127.0.0.1",
  idleTimeout: 60,
  routes: {
    // JSON API and the scoped, read-only board stream facade share the handler.
    "/v1/*": (request: Request) => app.fetch(request),
    "/streams/*": (request: Request) => app.fetch(request),
    "/openapi.json": (request: Request) => app.fetch(request),
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

const shutdown = async (): Promise<void> => {
  await server.stop(true);
  await app.close();
  await storage.close();
  if (deleteDbOnExit && dbPath !== ":memory:") {
    for (const path of [dbPath, `${dbPath}-shm`, `${dbPath}-wal`]) rmSync(path, { force: true });
  }
  process.exit(0);
};
process.once("SIGTERM", () => void shutdown());
process.once("SIGINT", () => void shutdown());

console.log(`LISTENING ${server.port}`);

export { server };
