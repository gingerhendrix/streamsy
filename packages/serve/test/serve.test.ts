import { Protocol } from "@streamsy/core/protocol";
import { routes as protocolRoutes } from "@streamsy/core/protocol";
import { Streamsy } from "@streamsy/core";
/* oxlint-disable effecttsgo/async-function -- These request tests own web handler disposal at the Web boundary. */
import { expect, test } from "bun:test";
import { Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Streams } from "@streamsy/core/protocol";
import { Serve } from "../src/index.ts";
import * as Contract from "../src/contract.ts";

class stream extends Streamsy.json<stream>()("serve/serve.test/stream", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}
class rows extends Streamsy.state<rows>()("serve/serve.test/rows", "rows/:seat", {
  params: { seat: Schema.String },
  collections: { cards: { schema: Schema.Struct({ id: Schema.String }), key: "id" } },
}) {}
const protocol = Streams.layerMemory();
const storage = Protocol.bind(protocol, Streamsy.group(stream, rows)).pipe(
  Layer.provideMerge(protocol),
);
class Seat extends Context.Service<Seat, { readonly seat: string }>()("test/Seat") {}
const auth = HttpRouter.middleware<{ provides: Seat }>()((handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.headers.authorization !== "Bearer seat")
      return HttpServerResponse.empty({ status: 401 });
    return yield* handler.pipe(Effect.provideService(Seat, { seat: "mine" }));
  }),
);

function app() {
  return HttpRouter.toWebHandler(
    Layer.mergeAll(
      protocolRoutes(),
      Serve.mount(stream, "/feed/:seat"),
      Serve.mount(rows, "/state/:seat"),
      Serve.route(
        "/me",
        Effect.flatMap(Seat, (p) => Serve.stream(stream.at(p))),
      ).pipe(Layer.provide(auth.layer)),
    ).pipe(HttpRouter.provideRequest(storage)),
    { disableLogger: true },
  );
}

test("Serve owns every method beneath the root protocol wildcard", async () => {
  const web = app();
  try {
    await web.handler(
      new Request("http://host/events/a", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: '"one"',
      }),
    );
    expect(await (await web.handler(new Request("http://host/feed/a"))).json()).toEqual(["one"]);
    const head = await web.handler(new Request("http://host/feed/a", { method: "HEAD" }));
    expect(head.status).toBe(200);
    expect(head.headers.get(Contract.STREAM_VERSION_HEADER)).toBe("1");
    expect(await head.text()).toBe("");
    for (const method of ["POST", "PUT", "DELETE", "OPTIONS"]) {
      const response = await web.handler(new Request("http://host/feed/a", { method }));
      expect(response.status).toBe(405);
      expect(response.headers.get("allow")).toBe("GET, HEAD");
    }
    expect(
      (await web.handler(new Request("http://host/events/a", { method: "HEAD" }))).status,
    ).toBe(200);
  } finally {
    await web.dispose();
  }
});

test("an address handler reads the middleware seat and unauthenticated requests get 401", async () => {
  const App = Layer.mergeAll(
    protocolRoutes({ prefix: "/_seed" }),
    Serve.route(
      "/me",
      Effect.flatMap(Seat, (p) => Serve.stream(stream.at(p))),
    ),
  ).pipe(Layer.provide(auth.layer), HttpRouter.provideRequest(storage));
  const web = HttpRouter.toWebHandler(App, { disableLogger: true });
  try {
    const seed = await web.handler(
      new Request("http://host/_seed/events/mine", {
        method: "PUT",
        headers: { authorization: "Bearer seat", "content-type": "application/json" },
        body: '"private-seat-item"',
      }),
    );
    expect(seed.status).toBe(201);
    expect((await web.handler(new Request("http://host/me"))).status).toBe(401);
    expect((await web.handler(new Request("http://host/unknown"))).status).toBe(404);
    const response = await web.handler(
      new Request("http://host/me", { headers: { authorization: "Bearer seat" } }),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(["private-seat-item"]);
    expect(response.headers.get(Contract.STREAM_CONTRACT_HEADER)).toMatch(/^[0-9a-f]{8}$/);
  } finally {
    await web.dispose();
  }
});

test("state has collection fingerprint, replay headers and no snapshot reset", async () => {
  const web = app();
  try {
    await web.handler(
      new Request("http://host/rows/a", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: "[]",
      }),
    );
    const response = await web.handler(new Request("http://host/state/a?offset=-1"));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual([]);
    expect(response.headers.get(Contract.STATE_VERSION_HEADER)).toBe("1");
    expect(
      (
        await web.handler(
          new Request("http://host/state/a", { headers: { "x-streamsy-state-reset": "snapshot" } }),
        )
      ).status,
    ).toBe(400);
    const invalid = await web.handler(new Request("http://host/state/a?offset=invalid"));
    expect(invalid.status).toBe(409);
    expect(await invalid.json()).toMatchObject({
      _tag: "ResumeRejected",
      recovery: "replay-from-start",
    });
    expect((await web.handler(new Request("http://host/state/a?cursor=invalid"))).status).toBe(400);
  } finally {
    await web.dispose();
  }
});

test("value encodes canonical JSON and conditional HEAD", async () => {
  const web = HttpRouter.toWebHandler(
    Serve.route(
      "/value",
      Serve.value(
        Schema.Struct({ b: Schema.Finite, a: Schema.String }),
        Effect.succeed({ b: 2, a: "one" }),
      ),
    ),
    { disableLogger: true },
  );
  try {
    const response = await web.handler(new Request("http://host/value"));
    expect(await response.text()).toBe('{"a":"one","b":2}');
    const etag = response.headers.get("etag")!;
    const cached = await web.handler(
      new Request("http://host/value", {
        method: "HEAD",
        headers: { "if-none-match": `W/${etag}` },
      }),
    );
    expect(cached.status).toBe(304);
    expect(await cached.text()).toBe("");
  } finally {
    await web.dispose();
  }
});

test("path codecs transform strings and reject invalid requests", async () => {
  class numbered extends Streamsy.json<numbered>()("serve/serve.test/numbered", "numbers/:n", {
    params: { n: Schema.FiniteFromString },
    schema: Schema.String,
  }) {}
  const path: `/${string}` = "/numbers/:n";
  const web = HttpRouter.toWebHandler(
    Serve.mount(numbered, path).pipe(
      HttpRouter.provideRequest(Protocol.bind(protocol, Streamsy.group(numbered))),
    ),
    { disableLogger: true },
  );
  try {
    expect((await web.handler(new Request("http://host/numbers/no"))).status).toBe(400);
    expect((await web.handler(new Request("http://host/numbers/1"))).status).toBe(404);
  } finally {
    await web.dispose();
  }
});

test("duplicate route shapes fail application construction", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(Serve.mount(stream, "/feed/:seat"), Serve.mount(stream, "/feed/:seat")).pipe(
      HttpRouter.provideRequest(storage),
    ),
    { disableLogger: true },
  );
  try {
    let rejected = false;
    try {
      await web.handler(new Request("http://host/feed/a"));
    } catch {
      rejected = true;
    }
    expect(rejected).toBe(true);
  } finally {
    await web.dispose();
  }
});

test("decoded stream parameters differ from raw protocol ids for encoded segments", async () => {
  const web = app();
  try {
    for (const value of ["w 1", "α"]) {
      const segment = encodeURIComponent(value);
      expect(
        (
          await web.handler(
            new Request(`http://host/events/${segment}`, {
              method: "PUT",
              headers: { "content-type": "application/json" },
              body: '"raw-id"',
            }),
          )
        ).status,
      ).toBe(201);
      expect(
        await (await web.handler(new Request(`http://host/events/${segment}`))).json(),
      ).toEqual(["raw-id"]);
      expect((await web.handler(new Request(`http://host/feed/${segment}`))).status).toBe(404);
    }
  } finally {
    await web.dispose();
  }
});

test("CORS handles Serve preflight and exposes cursor and contract headers", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      Serve.mount(stream, "/feed/:seat"),
      HttpRouter.cors({
        allowedOrigins: ["https://app.example.com"],
        allowedMethods: ["GET", "HEAD", "OPTIONS"],
        allowedHeaders: [Contract.STREAM_CONTRACT_HEADER],
        exposedHeaders: [
          "stream-next-offset",
          "stream-cursor",
          "stream-up-to-date",
          "etag",
          Contract.STREAM_CONTRACT_HEADER,
        ],
      }),
    ).pipe(HttpRouter.provideRequest(storage)),
    { disableLogger: true },
  );
  try {
    const response = await web.handler(
      new Request("http://host/feed/a", {
        method: "OPTIONS",
        headers: {
          origin: "https://app.example.com",
          "access-control-request-method": "GET",
          "access-control-request-headers": Contract.STREAM_CONTRACT_HEADER,
        },
      }),
    );
    expect(response.status).toBe(204);
    expect(response.headers.get("access-control-allow-headers")).toContain(
      Contract.STREAM_CONTRACT_HEADER,
    );
    const read = await web.handler(
      new Request("http://host/feed/a", { headers: { origin: "https://app.example.com" } }),
    );
    expect(read.headers.get("access-control-expose-headers")).toContain("stream-next-offset");
    expect(read.headers.get("access-control-expose-headers")).toContain(
      Contract.STREAM_CONTRACT_HEADER,
    );
  } finally {
    await web.dispose();
  }
});

test("fingerprints bind addresses, collection keys, and explicit contract revisions", async () => {
  class changed extends Streamsy.state<changed>()("serve/serve.test/changed", "rows/:seat", {
    params: { seat: Schema.String },
    collections: {
      cards: { schema: Schema.Struct({ id: Schema.String, other: Schema.String }), key: "other" },
    },
  }) {}
  const fingerprint = async (
    route: Layer.Layer<
      never,
      never,
      | HttpRouter.HttpRouter
      | HttpRouter.Request.From<"Requires", rows | changed>
      | HttpRouter.Request.From<"Error", Serve.ServeError>
    >,
    seat: string,
  ) => {
    const web = HttpRouter.toWebHandler(
      route.pipe(
        HttpRouter.provideRequest(
          Protocol.bind(protocol, Streamsy.group(changed)).pipe(Layer.provideMerge(storage)),
        ),
      ),
      {
        disableLogger: true,
      },
    );
    try {
      return (await web.handler(new Request(`http://host/state/${seat}`))).headers.get(
        Contract.STATE_CONTRACT_HEADER,
      );
    } finally {
      await web.dispose();
    }
  };
  const first = await fingerprint(Serve.mount(rows, "/state/:seat"), "a");
  expect(first).toMatch(/^[0-9a-f]{8}$/);
  expect(await fingerprint(Serve.mount(rows, "/state/:seat"), "a")).toBe(first);
  expect(await fingerprint(Serve.mount(rows, "/state/:seat"), "b")).not.toBe(first);
  expect(await fingerprint(Serve.mount(changed, "/state/:seat"), "a")).not.toBe(first);
  expect(await fingerprint(Serve.mount(rows, "/state/:seat", { contract: 2 }), "a")).not.toBe(
    first,
  );
});

test("handlers compose with app routes and apps can catch Serve errors", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      HttpRouter.add("GET", "/health", HttpServerResponse.text("ok")),
      HttpRouter.add(
        "GET",
        "/custom/:seat",
        Effect.flatMap(Serve.params(stream), (p) => Serve.stream(stream.at(p))).pipe(
          Effect.catchTag("ProtocolVersionUnsupported", () =>
            Effect.succeed(HttpServerResponse.text("Upgrade required", { status: 426 })),
          ),
        ),
      ),
    ).pipe(HttpRouter.provideRequest(storage)),
    { disableLogger: true },
  );
  try {
    expect(await (await web.handler(new Request("http://host/health"))).text()).toBe("ok");
    expect(
      (
        await web.handler(
          new Request("http://host/custom/a", {
            headers: { [Contract.STREAM_VERSION_HEADER]: "2" },
          }),
        )
      ).status,
    ).toBe(426);
    const rejected = await web.handler(new Request("http://host/custom/a?offset=invalid"));
    expect(rejected.status).toBe(409);
    expect(rejected.headers.get("cache-control")).toBe("no-store");
    expect(rejected.headers.get(Contract.STREAM_VERSION_HEADER)).toBe("1");
    expect(await rejected.json()).toEqual({
      _tag: "ResumeRejected",
      route: "/custom/:seat",
      reason: "invalid-offset",
      recovery: "replay-from-start",
    });
  } finally {
    await web.dispose();
  }
});

test("value handlers retain compute errors and encode failures as typed errors", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      Serve.route(
        "/failed",
        Serve.value(
          Schema.String,
          Effect.fail(
            new Serve.TransportUnavailable({ route: "/failed", detail: "Custom compute failure" }),
          ),
        ).pipe(
          Effect.catchTag("TransportUnavailable", () =>
            Effect.succeed(HttpServerResponse.empty({ status: 502 })),
          ),
        ),
      ),
      Serve.route("/invalid", Serve.value(Schema.Finite, Effect.succeed(NaN))),
      Serve.route("/value", Serve.value(Schema.String, Effect.succeed("ok"))),
    ),
    { disableLogger: true },
  );
  try {
    expect((await web.handler(new Request("http://host/failed"))).status).toBe(502);
    const invalid = await web.handler(new Request("http://host/invalid"));
    expect(invalid.status).toBe(500);
    expect(await invalid.json()).toEqual({
      _tag: "WireEncodeFailed",
      route: "/invalid",
      detail: "Value cannot be encoded",
    });
    const changed = await web.handler(
      new Request("http://host/value", { headers: { [Contract.VALUE_CONTRACT_HEADER]: "old" } }),
    );
    expect(changed.status).toBe(409);
    expect(changed.headers.get(Contract.VALUE_CONTRACT_HEADER)).toMatch(/^[0-9a-f]{8}$/);
    expect(await changed.json()).toEqual({
      _tag: "ContractChanged",
      route: "/value",
      recovery: "refetch",
    });
  } finally {
    await web.dispose();
  }
});
