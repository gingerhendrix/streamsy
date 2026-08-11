/**
 * Causal client reconciliation through `syncedThrough(ack)`.
 *
 * A command acknowledgement names the *canonical source* stream and offset. The
 * board is a separate projection that may lag. This helper waits until the board
 * projection proves it has incorporated an acknowledged canonical offset.
 *
 * The comparison itself is `@streamsy/experimental/causal`'s pure `coverage()`:
 * both sides are lifted into structured mesh identities and real durable-stream
 * positions, and coverage answers `proven`, `not-yet`, or `incomparable`.
 * `incomparable` is the important one — it is what a mismatched identity returns
 * instead of a comparison that might "resolve" by luck, and this module turns it
 * into a hard error rather than a retry. Nothing here ever compares a source
 * offset with a target offset; both operands are positions in the canonical
 * stream, and the identity must match before either is looked at.
 */

import {
  coverage,
  sourceAck,
  sourceWatermark,
  streamIdentityEquals,
} from "@streamsy/experimental/causal";

import { boardSourceIdentity } from "../../src/board/mesh.ts";
import type { HttpCall } from "../demo/bot.ts";

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
 * The canonical identity a stream id or game id names.
 *
 * A stream id is an application address; a mesh identity is what causal
 * comparison is defined over. Deriving one from the other in a single place is
 * what keeps a renamed stream from silently becoming a different causal subject.
 */
function canonicalIdentity(gameId: string | undefined, streamId: string) {
  return boardSourceIdentity(gameId ?? gameIdFromStreamId(streamId));
}

/** `games/<id>/events` → `<id>`. */
function gameIdFromStreamId(streamId: string): string {
  return streamId.split("/")[1] ?? streamId;
}

/**
 * True once `board` proves it incorporates `ack`'s canonical offset. Throws
 * {@link BoardSyncError} `wrong-stream` when the two name different canonical
 * identities, which `coverage()` reports as `incomparable`.
 */
export function boardIncorporates(board: BoardWatermark, ack: CommandAckRef): boolean {
  // Identity is resolved and compared first, before any position is looked at
  // and before the board has necessarily materialized. Comparing raw stream ids
  // here instead would miss the case the structured identity exists to catch:
  // two sides naming the same stream id for different games. A mismatch is
  // never lag, so it must not be reported as "not yet".
  const boardIdentity = canonicalIdentity(board.gameId, board.sourceStreamId);
  const ackIdentity = canonicalIdentity(ack.gameId, ack.sourceStreamId);
  if (!streamIdentityEquals(boardIdentity, ackIdentity)) throw wrongStream(board, ack);

  if (board.sourceThroughOffset === null) return false; // nothing incorporated yet

  const { status } = coverage(
    sourceWatermark(boardIdentity, board.sourceThroughOffset),
    sourceAck(ackIdentity, ack.sourceOffset),
  );
  // Identity already agreed, so `incomparable` is unreachable here; it is still
  // refused rather than folded into `false`, because a silent `not-yet` on an
  // uncomparable pair is exactly the bug this module exists to prevent.
  if (status === "incomparable") throw wrongStream(board, ack);
  return status === "proven";
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
    return {
      sourceStreamId: res.body.sourceStreamId,
      sourceThroughOffset: res.body.sourceThroughOffset ?? null,
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
