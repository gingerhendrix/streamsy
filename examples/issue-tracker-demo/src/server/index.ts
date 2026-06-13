import index from "../../index.html";
import { createApiRouter } from "./api.ts";
import { port, serverIdleTimeoutSeconds, streamPath } from "./config.ts";
import { json } from "./http.ts";
import { seed } from "./state.ts";
import { DemoStreams } from "./streams.ts";

const streams = new DemoStreams();
await streams.start();
await seed(streams);

const routeApi = createApiRouter(streams);

const server = Bun.serve({
  port,
  idleTimeout: serverIdleTimeoutSeconds,
  routes: {
    "/streams/*": (request) => streams.proxy(request),
    "/api/*": (request) => routeApi(request, new URL(request.url)),
    "/*": index,
  },
  error(error) {
    console.error(error);
    return json({ error: "Internal server error" }, { status: 500 });
  },
});

console.log(`Issue tracker demo listening on http://localhost:${server.port}`);
console.log(`Streamsy durable state stream: http://localhost:${server.port}${streamPath}`);

export { server };
