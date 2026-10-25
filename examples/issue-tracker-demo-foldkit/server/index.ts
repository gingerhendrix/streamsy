import { BunCrypto, BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
// The base app's server layers. This variant changes only the page.
import { ApiLive, ApiNotFoundLive } from "issue-tracker-demo/server/api.ts";
import { ServerConfig, serverIdleTimeoutSeconds } from "issue-tracker-demo/server/config.ts";
import { IssueTrackerLive, SeedLive } from "issue-tracker-demo/server/issue-tracker.ts";
import { StorageLive, Workspaces } from "issue-tracker-demo/server/streams.ts";
// The Foldkit page. Bun bundles its scripts and the base app's stylesheet.
import index from "../public/index.html";

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
        "/*": index,
      },
      // No hot module reloading. Foldkit takes `import.meta.hot` to be Vite's
      // HMR API, and Bun's `import.meta.hot` refuses the calls it makes.
      // Browser console logs are still echoed to the terminal.
      development: isDevelopment && { hmr: false, console: true },
    }),
  ),
);

/** One router: the JSON API beside the read-only Durable State route. */
const RoutesLive = Layer.mergeAll(
  ApiLive,
  ApiNotFoundLive,
  Serve.mount(Workspaces, "/streams/workspace/:workspaceId"),
);

// The base app's layer graph: storage opens first, `main` is seeded, then the
// listener starts. Shutdown runs in the reverse order.
const ServerLive = Serve.layer(RoutesLive, { disableLogger: true }).pipe(
  Layer.provide(ListenerLive),
  Layer.provide(SeedLive),
  Layer.provide(IssueTrackerLive),
  Layer.provide([StorageLive, BunCrypto.layer]),
);

BunRuntime.runMain(Layer.launch(ServerLive), { teardown: Serve.teardown });
