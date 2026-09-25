/* oxlint-disable effecttsgo/async-function -- Bun tests own the runtime lifetime. */
import { expect, test } from "bun:test";
import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Option } from "effect";
import { applyChange, newestRows, StoryRows } from "../../src/server/newest-rows.ts";
import { HackerNewsStories, storiesAddress } from "../../src/server/stories.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";

test("fixed State declaration owns only hackernews/newest", () => {
  expect(String(storiesAddress.id)).toBe("hackernews/newest");
  expect(HackerNewsStories.match("hackernews/newest")).toBe(true);
  expect(HackerNewsStories.match("hackernews/other")).toBe(false);
  expect(storiesAddress.collections["hn-story"].key).toBe("id");
});

test("applyChange replaces upserts and removes rows without mutating its input", () => {
  const row = story(101, 30, "first");
  const updated = { ...row, title: "updated" };
  const events = DurableState.changes(storiesAddress, { offset: ZERO_OFFSET }, [
    DurableState.upsert("hn-story", updated),
    DurableState.remove("hn-story", updated),
  ]);
  const before = { "101": row };
  const next = applyChange(before, events[0]);
  expect(next).toEqual({ "101": updated });
  expect(applyChange(next, events[1])).toEqual({});
  expect(before).toEqual({ "101": row });
});

test("fold resumes across pages, accepts the tail, and writes no extra stream events", async () => {
  const h = await demoHarness(1);
  try {
    expect((await h.runtime.runPromise(Projection.run(newestRows))).record.inputs.input).toBe(
      ZERO_OFFSET,
    );
    const first = story(101, 30, "first");
    const second = story(102, 20, "second");
    await h.append([
      DurableState.upsert("hn-story", first),
      DurableState.upsert("hn-story", second),
    ]);
    expect(await h.runtime.runPromise(Projection.run(newestRows))).toMatchObject({
      items: 2,
      units: 2,
    });
    const ack = await h.append([
      DurableState.remove("hn-story", first),
      DurableState.upsert("hn-story", { ...second, title: "updated" }),
    ]);
    const progress = await h.runtime.runPromise(Projection.run({ ...newestRows }));
    expect(progress.record.inputs.input).toBe(ack.offset);
    expect(
      Option.getOrThrow(await h.runtime.runPromise(Projection.loadState(newestRows, StoryRows))),
    ).toEqual({ "102": { ...second, title: "updated" } });
    expect(await h.runtime.runPromise(Projection.run(newestRows))).toMatchObject({
      items: 0,
      units: 0,
    });
    expect(await h.read()).toHaveLength(4);
  } finally {
    await h.close();
  }
});
