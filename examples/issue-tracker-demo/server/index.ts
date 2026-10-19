import { BunCrypto, BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import index from "../public/index.html";
import { ApiLive, ApiNotFoundLive } from "./api.ts";
import { ServerConfig, serverIdleTimeoutSeconds } from "./config.ts";
import { IssueTrackerLive, SeedLive } from "./issue-tracker.ts";
import { StorageLive, Workspaces } from "./streams.ts";

/** Bun serves the page; `/api` and `/streams` fall through to the Effect router. */
const ListenerLive = Layer.unwrap(
  Effect.map(ServerConfig, ({ port, isDevelopment }) =>
    BunHttpServer.layer({
      port,
      idleTimeout: serverIdleTimeoutSeconds,
      // The listener stops from its own scope once the open replies are sent.
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

/** One router: the JSON API beside the read-only Durable State route. */
const RoutesLive = Layer.mergeAll(
  ApiLive,
  ApiNotFoundLive,
  Serve.mount(Workspaces, "/streams/workspace/:workspaceId"),
);

// `Layer.provide` builds its argument first and releases it last. Storage opens
// first and the main workspace is seeded before the listener starts. On
// shutdown `Serve.layer` answers parked long polls with 204, then the
// listener stops, and storage closes last.
const ServerLive = Serve.layer(RoutesLive, { disableLogger: true }).pipe(
  Layer.provide(ListenerLive),
  Layer.provide(SeedLive),
  Layer.provide(IssueTrackerLive),
  Layer.provide([StorageLive, BunCrypto.layer]),
);

BunRuntime.runMain(Layer.launch(ServerLive), { teardown: Serve.teardown });
