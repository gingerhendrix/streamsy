/* oxlint-disable effecttsgo/async-function -- Bun tests own the runtime lifetime. */
import { expect, test } from "vitest";
import { DurableState, StreamAddress, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { applyChange, newestRows } from "../../src/server/newest-rows.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";

test("fold resumes across pages, accepts the tail, and writes no extra stream events", async () => {
  const h = await demoHarness(1);

  try {
    expect((await h.runtime.runPromise(Projection.run(newestRows))).offsets.input).toBe(
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
      DurableState.remove("hn-story", first.id, { oldValue: first }),
      DurableState.upsert("hn-story", { ...second, title: "updated" }),
    ]);

    const progress = await h.runtime.runPromise(Projection.run({ ...newestRows }));
    expect(progress.offsets.input).toBe(ack.offset);
    expect(await h.runtime.runPromise(Projection.read(newestRows))).toEqual({
      state: { "102": { ...second, title: "updated" } },
      offsets: { input: ack.offset },
    });
    expect(await h.runtime.runPromise(Projection.run(newestRows))).toMatchObject({
      items: 0,
      units: 0,
    });
    const events = await h.read();
    expect(events).toHaveLength(4);
    // The fold and the poller's reconcile share applyChange, which must copy its input.
    const rows = { "101": first };
    expect(applyChange(rows, events[2])).toEqual({});
    expect(rows).toEqual({ "101": first });
  } finally {
    await h.close();
  }
});

test("fold clears the rows at a stored reset and keeps the changes after it", async () => {
  const h = await demoHarness();

  try {
    const first = story(101, 30, "first");
    const second = story(102, 20, "second");
    await h.append([DurableState.upsert("hn-story", first)]);

    const ack = await h.append([
      StreamAddress.snapshotStart(),
      StreamAddress.reset(),
      DurableState.upsert("hn-story", second),
      StreamAddress.snapshotEnd(),
    ]);

    await h.runtime.runPromise(Projection.run(newestRows));
    expect(await h.runtime.runPromise(Projection.read(newestRows))).toEqual({
      state: { "102": second },
      offsets: { input: ack.offset },
    });

    const rows = { "101": first };
    expect(applyChange(rows, StreamAddress.reset())).toEqual({});
    expect(applyChange(rows, StreamAddress.snapshotEnd())).toBe(rows);
  } finally {
    await h.close();
  }
});
