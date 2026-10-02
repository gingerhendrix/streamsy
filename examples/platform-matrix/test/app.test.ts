// oxlint-disable effecttsgo/async-function -- Web boundary tests use requests and response body promises.
import { afterEach, expect, test } from "bun:test";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Http } from "@streamsy/core";
import { ConfigProvider, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import * as App from "../src/app.ts";

const edges: Array<{ readonly dispose: () => Promise<void> }> = [];

afterEach(async () => {
  await Promise.all(edges.splice(0).map((edge) => edge.dispose()));
});

/** The app as one web handler, with config from `env` only. */
const app = (env: Record<string, string> = {}) => {
  const edge = HttpRouter.toWebHandler(
    App.routes({
      runtime: "test",
      drivers: { sqlite: (filename) => SqliteClient.make({ filename }) },
    }).pipe(
      Layer.provide(Http.layerShutdownSignal),
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
    ),
    { disableLogger: true },
  );
  edges.push(edge);
  return edge.handler;
};

test("/healthz lists the mounted backends and the reason for each other one", async () => {
  const response = await app()(new Request("http://matrix/healthz"));
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    runtime: "test",
    backends: ["memory"],
    unmounted: {
      sqlite: "SQLITE_PATH is not set",
      pg: "STREAMSY_PG_URL is not set",
      mysql: "STREAMSY_MYSQL_URL is not set",
    },
  });
});

test("an unknown prefix is 404", async () => {
  const handler = app();
  const response = await handler(
    new Request("http://matrix/nope/s", {
      method: "PUT",
      headers: { "content-type": "text/plain" },
    }),
  );
  expect(response.status).toBe(404);
  expect((await handler(new Request("http://matrix/nope"))).status).toBe(404);
});

test.each(["sqlite", "pg", "mysql"])("%s without config is not mounted", async (backend) => {
  const response = await app()(
    new Request(`http://matrix/${backend}/s`, {
      method: "PUT",
      headers: { "content-type": "text/plain" },
    }),
  );
  expect(response.status).toBe(404);
});

test("a stream under /memory can be created and read", async () => {
  const handler = app();
  const created = await handler(
    new Request("http://matrix/memory/orders/1", {
      method: "PUT",
      headers: { "content-type": "text/plain" },
      body: "hello",
    }),
  );
  expect(created.status).toBe(201);
  expect(created.headers.get("location")).toBe("http://matrix/memory/orders/1");

  const read = await handler(new Request("http://matrix/memory/orders/1?offset=-1"));
  expect(read.status).toBe(200);
  expect(await read.text()).toBe("hello");
});
