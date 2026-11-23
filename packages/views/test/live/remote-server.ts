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

const server = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch: (request) => edge.handler(request),
});

// oxlint-disable-next-line effecttsgo/global-console -- The parent reads the server address from stdout.
console.log(new URL("/streams", server.url).href);
