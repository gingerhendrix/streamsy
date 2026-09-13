/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real Bun/Web host boundary. */
import { expect, test } from "bun:test";
import { Effect, Layer, Schema } from "effect";
import { Backend, StreamRoute, Streams } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { start } from "@streamsy/serve/bun";

const MemoryFamily = StreamRoute.json("memory/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
});
const SqliteFamily = StreamRoute.json("sqlite/:name", {
  params: { name: Schema.String },
  schema: Schema.String,
});

const memory = Backend.make("host-memory");
const sqlite = Backend.make("host-sqlite");
const routed = Streams.layerRouted([memory.serves(MemoryFamily), sqlite.serves(SqliteFamily)]).pipe(
  Layer.provide(memory.layer(Streams.layerMemory())),
  Layer.provide(sqlite.layer(BunStorage.layerProtocol({ client: { filename: ":memory:" } }))),
);

test("one Http.app host serves routed memory and SQLite families", async () => {
  const host = await Effect.runPromise(start({ layer: routed, port: 0 }));
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
      status: 500,
      body: "",
    });
  } finally {
    await Effect.runPromise(host.stop);
  }
});
