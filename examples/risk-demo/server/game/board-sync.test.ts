/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
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

/**
 * The comparison is `coverage()` over structured mesh identities, not string
 * equality on stream ids. These tests are about that substitution: a refusal
 * must come from `incomparable`, and a sentinel must never be accepted as a
 * causal position at all.
 */
describe("structured causal comparison", () => {
  it("refuses a cross-game comparison as incomparable, not as lag", () => {
    const board: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    // Same offset on both sides: string comparison alone would say "proven".
    // Identity is checked first, so this is a refusal instead.
    expect(() => boardIncorporates(board, { sourceStreamId: OTHER, sourceOffset: OFF_42 })).toThrow(
      BoardSyncError,
    );
  });

  it("refuses a cross-game comparison even before the board has materialized", () => {
    const board: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: null };
    expect(() => boardIncorporates(board, { sourceStreamId: OTHER, sourceOffset: OFF_42 })).toThrow(
      BoardSyncError,
    );
  });

  it("refuses a game-id mismatch the stream ids agree about, materialized or not", () => {
    // Identical stream ids, different games. Comparing the strings would call
    // this a match; resolving identities first is what catches it. Both the
    // materialized and the not-yet-materialized paths must refuse, or a caller
    // would poll a board that can never prove its ack until it times out.
    const crossGameAck = { sourceStreamId: SOURCE, sourceOffset: OFF_42, gameId: "game-2" };
    for (const sourceThroughOffset of [OFF_42, null]) {
      expect(() =>
        boardIncorporates(
          { sourceStreamId: SOURCE, sourceThroughOffset, gameId: "game-1" },
          crossGameAck,
        ),
      ).toThrow(BoardSyncError);
    }
  });

  it("accepts an explicit game id that agrees, whatever the stream id spelling", () => {
    const board: BoardWatermark = {
      sourceStreamId: "some/other/address",
      sourceThroughOffset: OFF_42,
      gameId: "game-1",
    };
    expect(
      boardIncorporates(board, { sourceStreamId: SOURCE, sourceOffset: OFF_40, gameId: "game-1" }),
    ).toBe(true);
  });

  it("rejects protocol read sentinels as causal positions", () => {
    const board: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    // `-1` and `now` are read offsets, not positions. Comparing them would be
    // meaningless, so construction refuses rather than producing a verdict.
    for (const sentinel of ["-1", "now"]) {
      expect(() =>
        boardIncorporates(board, { sourceStreamId: SOURCE, sourceOffset: sentinel }),
      ).toThrow(TypeError);
      expect(() =>
        boardIncorporates({ sourceStreamId: SOURCE, sourceThroughOffset: sentinel }, ack),
      ).toThrow(TypeError);
    }
  });

  it("proves an ack at exactly the watermark, and not one beyond it", () => {
    const board: BoardWatermark = { sourceStreamId: SOURCE, sourceThroughOffset: OFF_42 };
    expect(boardIncorporates(board, { sourceStreamId: SOURCE, sourceOffset: OFF_42 })).toBe(true);
    expect(boardIncorporates(board, { sourceStreamId: SOURCE, sourceOffset: OFF_40 })).toBe(true);
    expect(boardIncorporates({ sourceStreamId: SOURCE, sourceThroughOffset: OFF_40 }, ack)).toBe(
      false,
    );
  });

  it("waits rather than refusing when the identities agree", async () => {
    const timers = fakeTimers();
    let reads = 0;
    const board = await waitForBoardThrough(
      async () => ({
        sourceStreamId: SOURCE,
        sourceThroughOffset: (reads += 1) < 3 ? OFF_40 : OFF_42,
      }),
      ack,
      { ...timers, timeoutMs: 1_000 },
    );
    expect(board.sourceThroughOffset).toBe(OFF_42);
    expect(reads).toBe(3);
  });
});
