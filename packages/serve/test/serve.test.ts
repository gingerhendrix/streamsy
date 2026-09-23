/* oxlint-disable effecttsgo/async-function -- These request tests own web handler disposal at the Web boundary. */
import { expect, test } from "bun:test";
import { Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Http, StreamRoute, Streams, type StreamsReader } from "@streamsy/core";
import { Serve } from "../src/index.ts";
import * as Contract from "../src/contract.ts";

const family = StreamRoute.json("events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
});
const rows = StreamRoute.state("rows/:seat", {
  params: { seat: Schema.String },
  collections: { cards: { schema: Schema.Struct({ id: Schema.String }), key: "id" } },
});
const storage = Streams.layerMemory();
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
      Http.routes(),
      Serve.stream(family, "/feed/:seat"),
      Serve.state(rows, "/state/:seat"),
      Serve.stream(family, "/me", { params: Seat }).pipe(Layer.provide(auth.layer)),
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

test("D5 params reads the middleware seat and unauthenticated requests get 401", async () => {
  const App = Layer.mergeAll(
    Http.routes({ prefix: "/_seed" }),
    Serve.stream(family, "/me", { params: Seat }),
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

test("prefix registers once; bare prefix is 400 and neighbouring paths are 404", async () => {
  const web = HttpRouter.toWebHandler(
    Http.routes({ prefix: "/streams/" }).pipe(HttpRouter.provideRequest(storage)),
    { disableLogger: true },
  );
  try {
    expect((await web.handler(new Request("http://host/streams"))).status).toBe(400);
    expect((await web.handler(new Request("http://host/streams/"))).status).toBe(400);
    expect((await web.handler(new Request("http://host/streams-other/x"))).status).toBe(404);
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

test("document encodes canonical JSON and conditional HEAD", async () => {
  const source = {
    id: "summary",
    paramSchema: Schema.Struct({}),
    schema: Schema.Struct({ b: Schema.Finite, a: Schema.String }),
    resolve: () => Effect.succeed({ b: 2, a: "one" }),
  };
  const web = HttpRouter.toWebHandler(Serve.document(source, "/document"), { disableLogger: true });
  try {
    const response = await web.handler(new Request("http://host/document"));
    expect(await response.text()).toBe('{"a":"one","b":2}');
    const etag = response.headers.get("etag")!;
    const cached = await web.handler(
      new Request("http://host/document", {
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

test("path codecs transform strings; widened paths check keys at registration", async () => {
  const numbered = StreamRoute.json("numbers/:n", {
    params: { n: Schema.FiniteFromString },
    schema: Schema.String,
  });
  const path: `/${string}` = "/numbers/:n";
  const web = HttpRouter.toWebHandler(
    Serve.stream(numbered, path).pipe(HttpRouter.provideRequest(storage)),
    { disableLogger: true },
  );
  try {
    expect((await web.handler(new Request("http://host/numbers/no"))).status).toBe(400);
    expect((await web.handler(new Request("http://host/numbers/1"))).status).toBe(404);
    const wrong: `/${string}` = "/numbers/:wrong";
    expect(() => Serve.stream(numbered, wrong)).toThrow();
  } finally {
    await web.dispose();
  }
});

test("duplicate route shapes fail application construction", async () => {
  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(Serve.stream(family, "/feed/:seat"), Serve.stream(family, "/feed/:seat")).pipe(
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

test("decoded family parameters differ from raw protocol ids for encoded segments", async () => {
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
      Serve.stream(family, "/feed/:seat"),
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

test("fingerprints bind members, collection keys, and explicit contract revisions", async () => {
  const changed = StreamRoute.state("rows/:seat", {
    params: { seat: Schema.String },
    collections: {
      cards: { schema: Schema.Struct({ id: Schema.String, other: Schema.String }), key: "other" },
    },
  });
  const fingerprint = async (
    route: Layer.Layer<
      never,
      never,
      HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", StreamsReader>
    >,
    seat: string,
  ) => {
    const web = HttpRouter.toWebHandler(route.pipe(HttpRouter.provideRequest(storage)), {
      disableLogger: true,
    });
    try {
      return (await web.handler(new Request(`http://host/state/${seat}`))).headers.get(
        Contract.STATE_CONTRACT_HEADER,
      );
    } finally {
      await web.dispose();
    }
  };
  const first = await fingerprint(Serve.state(rows, "/state/:seat"), "a");
  expect(first).toMatch(/^[0-9a-f]{8}$/);
  expect(await fingerprint(Serve.state(rows, "/state/:seat"), "a")).toBe(first);
  expect(await fingerprint(Serve.state(rows, "/state/:seat"), "b")).not.toBe(first);
  expect(await fingerprint(Serve.state(changed, "/state/:seat"), "a")).not.toBe(first);
  expect(await fingerprint(Serve.state(rows, "/state/:seat", { contract: 2 }), "a")).not.toBe(
    first,
  );
});
