/* oxlint-disable effecttsgo/async-function -- Bun tests own the Web handler and runtime lifetime. */
import { expect, test } from "bun:test";
import { DurableState } from "@streamsy/core";
import { HttpRouter } from "effect/unstable/http";
import { Effect, Layer, Schema } from "effect";
import { app, stateRoute } from "../../src/server/http.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";
import { initialCounters, NewestStoriesPoller } from "../../src/server/poller/contract.ts";
import { ApiStatus } from "../../src/state-schema.ts";

test("State is empty at boot, replays pages, resumes, and rejects writes", async () => {
  const h = await demoHarness(1);
  const web = HttpRouter.toWebHandler(stateRoute.pipe(HttpRouter.provideRequest(h.clientLayer)), {
    disableLogger: true,
  });
  const read = (query = "offset=-1") =>
    web.handler(new Request(`http://host/state/newest?${query}`));
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
    const cursor = initial.headers.get("stream-next-offset")!;
    await h.append([DurableState.remove("hn-story", row.id, { oldValue: row })]);
    expect(await (await read(`offset=${cursor}`)).json()).toMatchObject([
      { type: "hn-story", key: "101", old_value: row, headers: { operation: "delete" } },
    ]);
    for (const method of ["PUT", "POST", "DELETE"]) {
      expect((await web.handler(new Request("http://host/state/newest", { method }))).status).toBe(
        405,
      );
    }
    expect((await web.handler(new Request("http://host/state/unknown"))).status).toBe(404);
    expect(
      (
        await web.handler(
          new Request("http://host/state/newest?offset=-1", {
            headers: { "x-streamsy-state-reset": "snapshot" },
          }),
        )
      ).status,
    ).toBe(400);
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
  const web = HttpRouter.toWebHandler(app.pipe(HttpRouter.provideRequest(services)), {
    disableLogger: true,
  });
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
