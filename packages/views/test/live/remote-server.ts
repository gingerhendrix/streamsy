import { Http, MemoryStorage, Streamsy } from "@streamsy/core";
import { Layer } from "effect";
import { HttpRouter } from "effect/http";
import { Shop } from "../circuit/support/shop.ts";

const group = Streamsy.group(Shop);

const edge = HttpRouter.toWebHandler(
  Http.routes(group, { access: "full", prefix: "/streams" }).pipe(
    Layer.provideMerge(MemoryStorage.layer(group)),
  ),
  { disableLogger: true },
);

const refused = new Set<string>();

const server = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch: (request) => {
    const { method } = request;

    if (
      new URL(request.url).pathname === "/streams/shop/recovering" &&
      (method === "HEAD" || method === "GET") &&
      !refused.has(method)
    ) {
      refused.add(method);

      return new Response("Test server is temporarily unavailable", { status: 503 });
    }

    return edge.handler(request);
  },
});

// oxlint-disable-next-line effecttsgo/global-console -- The parent reads the server address from stdout.
console.log(new URL("/streams", server.url).href);
