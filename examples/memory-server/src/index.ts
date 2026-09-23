/* oxlint-disable effecttsgo/global-console, effecttsgo/process-env -- This executable owns the HTTP listener and process configuration. */
import { Streams } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import { listener } from "@streamsy/serve/bun";
import { Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";

const port = parseInt(process.env.PORT ?? "1337", 10);
const runtime = ManagedRuntime.make(
  HttpRouter.serve(Http.routes(), { disableLogger: true, disableListenLog: true }).pipe(
    Layer.provide(Streams.layerMemory()),
    Layer.provideMerge(
      listener({ port, hostname: "0.0.0.0", idleTimeout: 60, gracefulShutdownTimeout: 1000 }),
    ),
  ),
);
export const server = await runtime.runPromise(HttpServer.HttpServer);
console.log(`Memory server listening on ${HttpServer.formatAddress(server.address)}`);
let shuttingDown: Promise<void> | undefined;
export function shutdown(): Promise<void> {
  return (shuttingDown ??= runtime.dispose());
}
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => void shutdown().finally(() => process.exit(0)));
}
