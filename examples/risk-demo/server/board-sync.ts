/**
 * Causal client reconciliation — `syncedThrough(ack)` (Batch 5).
 *
 * A command acknowledgement names the *canonical source* stream and offset. The
 * board is a separate projection that may lag. This helper waits until the board
 * projection proves it has incorporated an acknowledged canonical offset, using
 * Streamsy offset ordering.
 *
 * Critically, it compares offsets ONLY within the same stream: it matches the
 * board's `sourceStreamId` against the ack's `sourceStreamId` first, and only
 * then compares `sourceThroughOffset` to `sourceOffset` with {@link compareOffsets}.
 * Comparing offsets from unrelated streams is meaningless, so a stream mismatch
 * is a hard error rather than a race that might "resolve" by luck.
 */

import { compareOffsets } from "@streamsy/core";

import type { HttpCall } from "./agent.ts";

/** The board projection's causal watermark (from `GET /board`). */
export interface BoardWatermark {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
}

/** The canonical acknowledgement a command returned. */
export interface CommandAckRef {
  sourceStreamId: string;
  sourceOffset: string;
}

export type BoardSyncCode = "wrong-stream" | "timeout" | "aborted";

export class BoardSyncError extends Error {
  constructor(
    readonly code: BoardSyncCode,
    message: string,
  ) {
    super(message);
    this.name = "BoardSyncError";
  }
}

export interface WaitOptions {
  timeoutMs?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
  /** Injectable clock (ms). Defaults to `Date.now`. */
  now?: () => number;
  /** Injectable delay. Defaults to `setTimeout`. */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * True once `board` proves it incorporates `ack`'s canonical offset. Throws
 * {@link BoardSyncError} `wrong-stream` if the board is a projection of a
 * different source stream than the ack references (never compares across streams).
 */
export function boardIncorporates(board: BoardWatermark, ack: CommandAckRef): boolean {
  if (board.sourceStreamId !== ack.sourceStreamId) {
    throw new BoardSyncError(
      "wrong-stream",
      `board projects "${board.sourceStreamId}" but the ack is for "${ack.sourceStreamId}"`,
    );
  }
  if (board.sourceThroughOffset === null) return false;
  return compareOffsets(board.sourceThroughOffset, ack.sourceOffset) >= 0;
}

/**
 * Poll `readBoard` until it proves the projection is synced through `ack`, or
 * reject with a {@link BoardSyncError}. `readBoard` returns the board's
 * `{ sourceStreamId, sourceThroughOffset }`.
 */
export async function waitForBoardThrough(
  readBoard: () => Promise<BoardWatermark>,
  ack: CommandAckRef,
  options: WaitOptions = {},
): Promise<BoardWatermark> {
  const timeoutMs = options.timeoutMs ?? 5_000;
  const pollIntervalMs = options.pollIntervalMs ?? 25;
  const now = options.now ?? (() => Date.now());
  const sleep = options.sleep ?? defaultSleep;
  const start = now();

  for (;;) {
    if (options.signal?.aborted) {
      throw new BoardSyncError("aborted", "board sync aborted");
    }
    const board = await readBoard();
    if (boardIncorporates(board, ack)) return board; // may throw wrong-stream
    if (now() - start >= timeoutMs) {
      throw new BoardSyncError(
        "timeout",
        `board did not reach ${ack.sourceOffset} within ${timeoutMs}ms ` +
          `(last watermark ${board.sourceThroughOffset ?? "∅"})`,
      );
    }
    await sleep(pollIntervalMs);
  }
}

/** A board watermark reader backed by `GET /v1/games/{gameId}/board`. */
export function boardWatermarkReader(
  call: HttpCall,
  gameId: string,
): () => Promise<BoardWatermark> {
  return async () => {
    const res = await call("GET", `/v1/games/${gameId}/board`);
    return {
      sourceStreamId: res.body.sourceStreamId,
      sourceThroughOffset: res.body.sourceThroughOffset ?? null,
    };
  };
}

/**
 * Typed convenience: `await syncedThrough(call, gameId, ack)` resolves once the
 * game's board projection has incorporated the acknowledged canonical offset.
 */
export function syncedThrough(
  call: HttpCall,
  gameId: string,
  ack: CommandAckRef,
  options?: WaitOptions,
): Promise<BoardWatermark> {
  return waitForBoardThrough(boardWatermarkReader(call, gameId), ack, options);
}
