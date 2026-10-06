/* oxlint-disable effecttsgo/async-function -- These tests own the Web request boundary. */
import { expect, test } from "bun:test";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRespondable, Etag } from "effect/http";
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/http-api";
import { BunServices, BunHttpPlatform } from "@effect/platform-bun";
import { Streamsy, MemoryStorage, Http } from "@streamsy/core";
import { Serve } from "../src/index.ts";
import * as Contract from "../src/contract.ts";

class Events extends Streamsy.json<Events>()("serve/httpapi/Events", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}
class Api extends HttpApi.make("ServeApi").add(
  HttpApiGroup.make("feed").add(
    HttpApiEndpoint.get("declared", "/declared/:seat", {
      params: { seat: Schema.String },
      error: [
        Serve.ProtocolVersionUnsupported,
        Serve.ResumeRejected,
        Serve.TransportUnavailable,
        Serve.WireEncodeFailed,
      ],
    }),
    HttpApiEndpoint.get("respondable", "/respondable/:seat", { params: { seat: Schema.String } }),
  ),
) {}
const Feed = HttpApiBuilder.group(Api, "feed", (handlers) =>
  handlers
    .handleRaw("declared", ({ params }) => Serve.stream(Events.at(params)))
    .handleRaw("respondable", ({ params }) =>
      Serve.stream(Events.at(params)).pipe(Effect.catch(HttpServerRespondable.toResponse)),
    ),
);

test("handleRaw serves stream responses; declared errors use HttpApi encoding", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      HttpApiBuilder.layer(Api),
      Http.routes(Streamsy.group(Events), { prefix: "/_seed", access: "full" }),
    ).pipe(
      Layer.provide(Feed),
      Layer.provide([BunServices.layer, BunHttpPlatform.layer, Etag.layer]),
      HttpRouter.provideRequest(MemoryStorage.layer(Streamsy.group(Events))),
    ),
    { disableLogger: true },
  );
  try {
    // An absent stream response passes through handleRaw.
    expect((await web.handler(new Request("http://host/declared/a"))).status).toBe(404);
    const seeded = await web.handler(
      new Request("http://host/_seed/events/a", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: '"one"',
      }),
    );
    expect(seeded.status).toBe(201);
    const read = await web.handler(new Request("http://host/declared/a?offset=-1"));
    expect(read.status).toBe(200);
    expect(await read.json()).toEqual(["one"]);
    expect(read.headers.get(Contract.STREAM_VERSION_HEADER)).toBe("1");
    for (const path of ["declared", "respondable"]) {
      const response = await web.handler(
        new Request(`http://host/${path}/a`, {
          headers: { [Contract.STREAM_VERSION_HEADER]: "2" },
        }),
      );
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({
        _tag: "ProtocolVersionUnsupported",
        route: `/${path}/:seat`,
        supported: 1,
        received: "2",
        recovery: "replay-from-start",
      });
      // HttpApi encodes declared errors itself. Adapt to a response to retain Serve headers.
      expect(response.headers.get(Contract.STREAM_VERSION_HEADER)).toBe(
        path === "respondable" ? "1" : null,
      );
      expect(response.headers.get("cache-control")).toBe(
        path === "respondable" ? "no-store" : null,
      );
    }
  } finally {
    await web.dispose();
  }
});
