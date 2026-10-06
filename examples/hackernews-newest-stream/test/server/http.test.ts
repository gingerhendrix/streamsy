/* oxlint-disable effecttsgo/async-function -- Bun tests own the Web handler and runtime lifetime. */
import { expect, test } from "bun:test";
import { BunHttpServer } from "@effect/platform-bun";
import { DurableState } from "@streamsy/core";
import { HttpRouter } from "effect/http";
import { Effect, Layer, Schema } from "effect";
import { app, stateRoute } from "../../src/server/http.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";
import { initialCounters, NewestStoriesPoller } from "../../src/server/poller/contract.ts";
import { ApiStatus } from "../../src/state-schema.ts";

test("State is empty at boot and serves rows in the State wire shape", async () => {
  const h = await demoHarness();

  const web = HttpRouter.toWebHandler(stateRoute.pipe(HttpRouter.provideRequest(h.clientLayer)), {
    disableLogger: true,
  });

  const read = () => web.handler(new Request("http://host/state/newest?offset=-1"));

  try {
    const empty = await read();
    expect(empty.status).toBe(200);
    expect(await empty.json()).toEqual([]);
    const row = story(101, 30, "First");
    await h.append([DurableState.upsert("hn-story", row)]);
    const initial = await read();
    expect(initial.headers.get("x-streamsy-state-version")).toBe("1");
    expect(initial.headers.get("x-streamsy-state-contract")).toBeTruthy();
    expect(await initial.json()).toMatchObject([
      { type: "hn-story", key: "101", value: row, headers: { operation: "upsert" } },
    ]);
  } finally {
    await web.dispose();
    await h.close();
  }
});

test("merged app serves status, poll, State and page routes", async () => {
  const h = await demoHarness();
  let polls = 0;

  const services = Layer.mergeAll(
    h.clientLayer,
    Layer.succeed(NewestStoriesPoller, {
      pollNow: Effect.sync(() => {
        polls++;
      }),
      stats: Effect.succeed({
        ...initialCounters,
        polling: false,
        newestLimit: 50,
        pollIntervalMs: 60_000,
      }),
    }),
  );

  const web = HttpRouter.toWebHandler(
    app.pipe(HttpRouter.provideRequest(services), Layer.provide(BunHttpServer.layerHttpServices)),
    { disableLogger: true },
  );

  try {
    const status = await web.handler(new Request("http://host/api/status"));
    expect(Schema.decodeUnknownSync(ApiStatus)(await status.json())).toMatchObject({
      streamPath: "/state/newest",
      newestLimit: 50,
      pollIntervalMs: 60_000,
      totalConflictsRetried: 0,
    });
    expect(
      (await web.handler(new Request("http://host/api/poll", { method: "POST" }))).status,
    ).toBe(200);
    expect(polls).toBe(1);
    const unknown = await web.handler(new Request("http://host/api/unknown"));
    expect(unknown.status).toBe(404);
    expect(await unknown.json()).toEqual({ error: "Not found" });
    expect(
      await (await web.handler(new Request("http://host/state/newest?offset=-1"))).json(),
    ).toEqual([]);
    const page = await web.handler(new Request("http://host/"));
    expect(page.status).toBe(200);
    expect(await page.text()).toMatch(
      /<div id="root"><\/div>|Hacker News newest stream API is running/,
    );
  } finally {
    await web.dispose();
    await h.close();
  }
});
