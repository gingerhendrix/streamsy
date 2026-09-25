import { BunCrypto, BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Streamsy } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import index from "../public/index.html";
import { ApiLive, ApiNotFoundLive } from "./api.ts";
import { ServerConfig, serverIdleTimeoutSeconds, streamPrefix } from "./config.ts";
import { IssueTrackerLive, SeedLive } from "./issue-tracker.ts";
import { StorageLive, Workspaces } from "./streams.ts";

/** Bun serves the page; `/api` and `/streams` fall through to the Effect router. */
const ListenerLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ port, isDevelopment }) =>
    BunHttpServer.layer({
      port,
      idleTimeout: serverIdleTimeoutSeconds,
      // On shutdown the router first interrupts open requests (a parked long
      // poll answers 503); the listener then stops once those replies are sent.
      disablePreemptiveShutdown: true,
      routes: {
        "/api/*": false,
        "/streams": false,
        "/streams/*": false,
        // Bun bundles the HTML import (scripts, styles, assets) and serves it
        // for every other path.
        "/*": index,
      },
      // Hot module reloading for the bundled client, and browser console logs
      // echoed to the terminal.
      development: isDevelopment && { hmr: true, console: true },
    }),
  ),
);

/** One router: the JSON API beside Streamsy's read-only stream endpoints. */
const RoutesLive = Layer.mergeAll(
  ApiLive,
  ApiNotFoundLive,
  Http.routes(Streamsy.group(Workspaces), { prefix: streamPrefix, access: "read" }),
);

// `Layer.provide` builds its argument first and releases it last. Storage opens
// first and the main workspace is seeded before the listener starts. On
// shutdown the listener stops before storage closes.
const ServerLive = HttpRouter.serve(RoutesLive, { disableLogger: true }).pipe(
  Layer.provide(ListenerLive),
  Layer.provide(SeedLive),
  Layer.provide(IssueTrackerLive),
  Layer.provide([StorageLive, BunCrypto.layer]),
);

BunRuntime.runMain(Layer.launch(ServerLive));
