/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real Bun/Web host boundary. */
import { expect, test } from "vitest";
import { Layer, Schema } from "effect";
import { Http, MemoryStorage, Streamsy } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { HttpRouter } from "effect/http";

class MemoryFamily extends Streamsy.json<MemoryFamily>()("MixedHost/Memory", "memory/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
}) {}

class SqliteFamily extends Streamsy.json<SqliteFamily>()("MixedHost/Sqlite", "sqlite/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
}) {}

const streams = Streamsy.group(MemoryFamily, SqliteFamily);

const hostLayer = Layer.merge(
  MemoryStorage.layer(Streamsy.group(MemoryFamily)),
  BunStorage.layer(Streamsy.group(SqliteFamily), { client: { filename: ":memory:" } }),
);

test("one Http.routes host serves memory and SQLite streams", async () => {
  const web = HttpRouter.toWebHandler(
    Http.routes(streams, { access: "full" }).pipe(HttpRouter.provideRequest(hostLayer)),
    { disableLogger: true },
  );

  const server = Bun.serve({ port: 0, fetch: (request) => web.handler(request) });
  const host = { url: server.url };

  try {
    for (const path of ["memory/notes", "sqlite/notes"]) {
      const created = await fetch(new URL(path, host.url), {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: '"first"',
      });

      expect(created.status).toBe(201);

      const appended = await fetch(new URL(path, host.url), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '["second"]',
      });

      expect(appended.status).toBe(204);

      const read = await fetch(new URL(path, host.url));
      expect(read.status).toBe(200);
      expect(await read.json()).toEqual(["first", "second"]);
    }

    const unrouted = await fetch(new URL("unrouted/notes", host.url));
    expect({ status: unrouted.status, body: await unrouted.text() }).toEqual({
      status: 404,
      body: "",
    });
  } finally {
    await server.stop(true);
    await web.dispose();
  }
});
