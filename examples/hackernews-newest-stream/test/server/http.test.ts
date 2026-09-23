/* oxlint-disable effecttsgo/async-function -- Bun tests own the Web handler and runtime lifetime. */
import { expect, test } from "bun:test";
import { Streams } from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { HttpRouter } from "effect/unstable/http";
import { Effect, Layer, Option, Schema } from "effect";
import { app, stateRoute } from "../../src/server/http.ts";
import { hackerNewsStoryIndex, hackerNewsTarget } from "../../src/server/story-index-projection.ts";
import { hackerNewsSource } from "../../src/server/stream-resources.ts";
import { sourceDelete, sourceUpsert } from "../../src/server/source-change.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";
import { initialCounters, NewestStoriesPoller } from "../../src/server/poller/contract.ts";
import { StoryProjection } from "../../src/server/projection.ts";
import { HackerNewsStateChange } from "../../src/state-schema.ts";

test("served rows read only outputs, replay pages, and resume at the cursor", async () => {
  const h = await demoHarness(1);
  const web = HttpRouter.toWebHandler(stateRoute.pipe(HttpRouter.provideRequest(h.clientLayer)), {
    disableLogger: true,
  });
  const read = (query = "offset=-1") =>
    web.handler(new Request(`http://host/state/newest?${query}`));
  try {
    expect((await read()).status).toBe(404);
    const checkpoints = await h.runtime.runPromise(Checkpoints);
    expect(
      Option.isNone(
        (await h.runtime.runPromise(checkpoints.load(Projection.key(hackerNewsStoryIndex)))).record,
      ),
    ).toBe(true);
    expect(
      await h.runtime.runPromise(Streams.head(hackerNewsTarget).pipe(Effect.flip)),
    ).toMatchObject({ _tag: "StreamNotFound" });
    const row = story(101, 1_700_000_030, "First");
    await h.append(hackerNewsSource.id, [sourceUpsert(row)]);
    await h.runtime.runPromise(Projection.run(hackerNewsStoryIndex));
    const initial = await read();
    expect(initial.status).toBe(200);
    expect(initial.headers.get("x-streamsy-state-version")).toBe("1");
    expect(initial.headers.get("x-streamsy-state-contract")).toBeTruthy();
    expect(
      Schema.decodeUnknownSync(Schema.Array(HackerNewsStateChange))(await initial.json()),
    ).toEqual([{ type: "hn-story", key: "101", value: row, headers: { operation: "upsert" } }]);
    const cursor = initial.headers.get("stream-next-offset")!;
    await h.append(hackerNewsSource.id, [sourceDelete(row)]);
    await h.runtime.runPromise(Projection.run(hackerNewsStoryIndex));
    await h.runtime.runPromise(Streams.remove(hackerNewsSource));
    const suffix = await read(`offset=${cursor}`);
    expect(await suffix.json()).toEqual([
      { type: "hn-story", key: "101", headers: { operation: "delete" } },
    ]);
    expect((await read()).status).toBe(200);
    expect(
      (await web.handler(new Request("http://host/state/newest", { method: "POST" }))).status,
    ).toBe(405);
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

test("merged app keeps API and State routes ahead of the static fallback", async () => {
  const h = await demoHarness();
  const services = Layer.mergeAll(
    h.clientLayer,
    Layer.succeed(NewestStoriesPoller, {
      pollNow: Effect.void,
      stats: Effect.succeed({
        ...initialCounters,
        polling: false,
        stopped: false,
        lastStoryCount: 0,
      }),
    }),
    Layer.succeed(StoryProjection, { status: Effect.succeed({ running: false }) }),
  );
  const web = HttpRouter.toWebHandler(app.pipe(HttpRouter.provideRequest(services)), {
    disableLogger: true,
  });
  try {
    const unknown = await web.handler(new Request("http://host/api/unknown"));
    expect(unknown.status).toBe(404);
    expect(unknown.headers.get("content-type")).toContain("application/json");
    expect(await unknown.json()).toEqual({ error: "Not found" });

    const row = story(101, 1_700_000_030, "Routed story");
    await h.append(hackerNewsSource.id, [sourceUpsert(row)]);
    await h.runtime.runPromise(Projection.run(hackerNewsStoryIndex));
    const state = await web.handler(new Request("http://host/state/newest?offset=-1"));
    expect(state.status).toBe(200);
    expect(state.headers.get("x-streamsy-state-version")).toBe("1");
    expect(await state.json()).toEqual([
      { type: "hn-story", key: "101", value: row, headers: { operation: "upsert" } },
    ]);

    const page = await web.handler(new Request("http://host/"));
    expect(page.status).toBe(200);
    // Both built-client and API-only checkouts must reach the static route.
    expect(await page.text()).toMatch(
      /<div id="root"><\/div>|Hacker News newest stream API is running/,
    );
  } finally {
    await web.dispose();
    await h.close();
  }
});
