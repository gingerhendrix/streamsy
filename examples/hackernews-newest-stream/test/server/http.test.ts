/* oxlint-disable effecttsgo/async-function -- Bun tests own the Web handler and runtime lifetime. */
import { expect, test } from "bun:test";
import { Streams } from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { HttpRouter } from "effect/unstable/http";
import { Effect, Option, Schema } from "effect";
import { stateRoute } from "../../src/server/http.ts";
import { hackerNewsStoryIndex, hackerNewsTarget } from "../../src/server/story-index-projection.ts";
import { hackerNewsSource } from "../../src/server/stream-resources.ts";
import { sourceDelete, sourceUpsert } from "../../src/server/source-change.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";
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
