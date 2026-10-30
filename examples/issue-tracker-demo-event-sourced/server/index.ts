import { BunCrypto, BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
// The base app's page, scripts and styles. This variant changes only the server.
import index from "issue-tracker-demo/public/index.html";
import { ApiLive, ApiNotFoundLive } from "./api.ts";
import { ServerConfig, serverIdleTimeoutSeconds } from "./config.ts";
import { BootLive, IssueTrackerLive } from "./issue-tracker.ts";
import { StorageLive, Workspaces } from "./streams.ts";
import { ProjectionWatchersLive } from "./watchers.ts";

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

/** One router: the JSON API beside the read-only State stream, at the base app's paths. */
const RoutesLive = Layer.mergeAll(
  ApiLive,
  ApiNotFoundLive,
  Serve.mount(Workspaces, "/streams/workspace/:workspaceId"),
);

// `Layer.provide` builds its argument first and releases it last. Storage and
// the checkpoint store open first. The watchers start next, and the boot step
// seeds `main` and starts a read model runner for each workspace before the
// listener starts. On shutdown the listener stops, the runners stop, and
// storage closes last.
const ServerLive = Serve.layer(RoutesLive).pipe(
  Layer.provide(ListenerLive),
  Layer.provide(BootLive),
  Layer.provide(IssueTrackerLive),
  Layer.provide(ProjectionWatchersLive),
  Layer.provide([StorageLive, BunCrypto.layer]),
);

BunRuntime.runMain(Layer.launch(ServerLive), { teardown: Serve.teardown });
