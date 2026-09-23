/* oxlint-disable effecttsgo/node-builtin-import -- The executable host owns its SQLite path. */
import { listener } from "@streamsy/serve/bun";
import { Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { app } from "./app.ts";
import { applicationLayer } from "./host.ts";

const port = Number(process.env.PORT ?? 1340);
const databasePath =
  process.env.ISSUE_TRACKER_DB ?? `/tmp/streamsy-issue-tracker-${process.pid}.sqlite`;
const workspaces = (process.env.ISSUE_TRACKER_WORKSPACES ?? "acme,live").split(",").filter(Boolean);
if (databasePath !== ":memory:") await mkdir(dirname(databasePath), { recursive: true });
const runtime = ManagedRuntime.make(
  HttpRouter.serve(app(workspaces), { disableLogger: true, disableListenLog: true }).pipe(
    Layer.provide(applicationLayer(databasePath, workspaces)),
    Layer.provideMerge(listener({ port })),
  ),
);
export const server = await runtime.runPromise(HttpServer.HttpServer);
console.log(`Issue tracker listening on ${HttpServer.formatAddress(server.address)}`);
let closing: Promise<void> | undefined;
export const shutdown = () => (closing ??= runtime.dispose());
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => void shutdown().finally(() => process.exit(0)));
