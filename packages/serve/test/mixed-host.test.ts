/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real Bun/Web host boundary. */
import { expect, test } from "bun:test";
import { Layer, Schema } from "effect";
import { Http, Memory, StreamFamily, StreamGroup } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { HttpRouter } from "effect/unstable/http";

class MemoryFamily extends StreamFamily.json<MemoryFamily>()("RoutedHost/Memory", "memory/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
}) {}
class SqliteFamily extends StreamFamily.json<SqliteFamily>()("RoutedHost/Sqlite", "sqlite/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
}) {}

const families = StreamGroup.make(MemoryFamily, SqliteFamily);
const hostLayer = Layer.merge(
  Memory.layer({}, MemoryFamily),
  BunStorage.layer({ client: { filename: ":memory:" } }, SqliteFamily),
);

test("one Http.routes host serves memory and SQLite families", async () => {
  const web = HttpRouter.toWebHandler(
    Http.routes(families, { access: "full" }).pipe(HttpRouter.provideRequest(hostLayer)),
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
