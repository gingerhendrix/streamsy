import index from "../public/index.html";
import {
  isDevelopment,
  mainWorkspaceId,
  port,
  serverIdleTimeoutSeconds,
  workspaceStreamId,
} from "./config.ts";
import { commentRoutes } from "./routes/comments.ts";
import { issueRoutes } from "./routes/issues.ts";
import { projectRoutes } from "./routes/projects.ts";
import { seedMainWorkspace } from "./state.ts";
import { DemoStreams } from "./streams.ts";
import { json, notFound } from "./utils.ts";
import { workspaceRoutes } from "./workspaces.ts";

const streams = new DemoStreams();

// Boot: ensure and seed the known demo workspace. Shared workspaces are
// created on demand via POST /api/workspaces; their streams are the only
// record that they exist.
await seedMainWorkspace(streams);

const server = Bun.serve({
  port,
  idleTimeout: serverIdleTimeoutSeconds,
  routes: {
    // Streamsy durable stream endpoints, served by @streamsy/core's HTTP
    // handler. One stream per workspace: /streams/workspace/<id>.
    "/streams/*": (request: Request) => streams.proxy(request),

    // API endpoints as Bun route objects: exact and parameterized routes with
    // per-HTTP-method handlers. Methods not defined on a route object fall
    // through to the "/api/*" JSON 404 below.
    ...workspaceRoutes(streams),
    ...projectRoutes(streams),
    ...issueRoutes(streams),
    ...commentRoutes(streams),

    // Unknown /api paths return JSON 404 instead of falling through to the SPA.
    "/api/*": () => notFound(),

    // SPA fallback: Bun bundles the HTML import (scripts, styles, assets) and
    // serves it for every other path.
    "/*": index,
  },
  development: isDevelopment && {
    // Hot module reloading for the bundled client.
    hmr: true,
    // Echo browser console logs to the terminal.
    console: true,
  },
  error(error) {
    console.error(error);
    if (isDevelopment) {
      return json({ error: error.message, stack: error.stack }, { status: 500 });
    }
    return json({ error: "Internal server error" }, { status: 500 });
  },
});

console.log(`Issue tracker demo listening on http://localhost:${server.port}`);
console.log(
  `Known workspace stream: http://localhost:${server.port}/streams/${workspaceStreamId(mainWorkspaceId)}`,
);

export { server };
