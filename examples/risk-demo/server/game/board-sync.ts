/* oxlint-disable effecttsgo/async-function -- This module preserves a public Promise compatibility facade over protocol/runtime-owned application work. */
/* oxlint-disable effecttsgo/global-date, effecttsgo/global-timers, effecttsgo/new-promise -- This Promise compatibility facade bridges protocol subscriptions and bounded waits while preserving its public timing and cancellation contract. */
/**
 * Causal client reconciliation through `syncedThrough(ack)`.
 *
 * A command acknowledgement names the *canonical source* stream and offset. The
 * board is a separate projection that may lag. This helper waits until the board
 * projection proves it has incorporated an acknowledged canonical offset.
 *
 * Only compare positions in the same canonical source.
 */

import { compare, Offset } from "@streamsy/core";
import { BoardResponse } from "../../src/application/api.ts";
import type { HttpCall } from "../demo/bot.ts";
import { Schema } from "effect";

/** The board projection's causal watermark (from `GET /board`). */
export interface BoardWatermark {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  /** The game whose canonical identity this board projects. */
  gameId?: string;
}

/** The canonical acknowledgement a command returned. */
export interface CommandAckRef {
  sourceStreamId: string;
  sourceOffset: string;
  gameId?: string;
}

export type BoardSyncCode = "wrong-stream" | "timeout" | "aborted";

// oxlint-disable-next-line effecttsgo/extends-native-error -- BoardSyncError is the documented rejected-Promise error inspected with instanceof by public callers.
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

/** True once this game's board incorporates the canonical command acknowledgement. */
export function boardIncorporates(board: BoardWatermark, ack: CommandAckRef): boolean {
  const boardGame = board.gameId ?? board.sourceStreamId.split("/")[1];
  const ackGame = ack.gameId ?? ack.sourceStreamId.split("/")[1];
  if (!boardGame || boardGame !== ackGame) throw wrongStream(board, ack);
  if (board.sourceThroughOffset === null) return false;
  const position = Schema.decodeUnknownSync(Offset);
  return compare(position(board.sourceThroughOffset), position(ack.sourceOffset)) >= 0;
}

function wrongStream(board: BoardWatermark, ack: CommandAckRef): BoardSyncError {
  return new BoardSyncError(
    "wrong-stream",
    `board projects "${board.sourceStreamId}" but the ack is for "${ack.sourceStreamId}"`,
  );
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
    const body = Schema.decodeUnknownSync(BoardResponse)(res.body);
    return {
      sourceStreamId: body.sourceStreamId,
      sourceThroughOffset: body.sourceThroughOffset ?? null,
      gameId,
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
