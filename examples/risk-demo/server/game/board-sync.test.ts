import { describe, expect, it } from "vitest";

import {
  BoardSyncError,
  boardIncorporates,
  waitForBoardThrough,
  type BoardWatermark,
} from "./board-sync.ts";

const SOURCE = "games/game-1/events";
const OTHER = "games/game-2/events";
// Two well-ordered canonical offsets in the default fixed-width format.
const OFF_40 = "0000000000000000_0000000000000040";
const OFF_42 = "0000000000000000_0000000000000042";

const ack = { sourceStreamId: SOURCE, sourceOffset: OFF_42 };

/** A fake clock + sleep so timeout behaviour is deterministic (no real waiting). */
function fakeTimers() {
  let t = 0;
  return {
    now: () => t,
    sleep: async (ms: number) => {
      t += ms;
    },
  };
}

describe("causal board sync (syncedThrough)", () => {
  it("resolves immediately when the board already incorporates the ack", async () => {
    const board: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    const result = await waitForBoardThrough(async () => board, ack, fakeTimers());
    expect(result.sourceThroughOffset).toBe(OFF_42);
  });

  it("resolves after the projection catches up (delayed)", async () => {
    const behind: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_40 };
    const caught: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    let reads = 0;
    const result = await waitForBoardThrough(async () => (++reads >= 3 ? caught : behind), ack, {
      ...fakeTimers(),
      pollIntervalMs: 5,
    });
    expect(result.sourceThroughOffset).toBe(OFF_42);
    expect(reads).toBe(3);
  });

  it("treats a not-yet-materialized (null watermark) board as not synced, then resolves", async () => {
    const empty: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: null };
    const caught: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    let reads = 0;
    const result = await waitForBoardThrough(async () => (++reads >= 2 ? caught : empty), ack, {
      ...fakeTimers(),
    });
    expect(result.sourceThroughOffset).toBe(OFF_42);
  });

  it("never compares unrelated streams — a wrong source stream is a hard error", async () => {
    const wrong: BoardWatermark = { sourceStreamId: OTHER, sourceThroughOffset: OFF_42 };
    await expect(waitForBoardThrough(async () => wrong, ack, fakeTimers())).rejects.toMatchObject({
      code: "wrong-stream",
    });
    // The pure predicate throws too, rather than returning a meaningless comparison.
    expect(() => boardIncorporates(wrong, ack)).toThrow(BoardSyncError);
  });

  it("times out when the projection never catches up", async () => {
    const behind: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_40 };
    await expect(
      waitForBoardThrough(async () => behind, ack, {
        ...fakeTimers(),
        timeoutMs: 50,
        pollIntervalMs: 10,
      }),
    ).rejects.toMatchObject({ code: "timeout" });
  });

  it("rejects when the abort signal is already aborted", async () => {
    const behind: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_40 };
    await expect(
      waitForBoardThrough(async () => behind, ack, {
        ...fakeTimers(),
        signal: AbortSignal.abort(),
      }),
    ).rejects.toMatchObject({ code: "aborted" });
  });
});
