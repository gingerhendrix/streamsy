/**
 * Board-projection generation rebuild + durable cutover (Batch 5).
 *
 * A generation is a *separate, rebuildable* Durable State board stream
 * (`games/<id>/projections/board/<generation>`). Rebuilding replays the whole
 * canonical event log into a fresh generation through the same replay-safe
 * {@link ProjectionRuntime}, verifies the result against the authoritative
 * aggregate fold (logical board equivalence + identical canonical
 * `sourceThroughOffset`), and only then atomically repoints the durable active
 * generation. Verification failure leaves the old active generation untouched
 * and usable; old generations are retained, never deleted, so a cutover is
 * reversible.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";
import type { ProjectionAdapter } from "@streamsy/experimental/projection";

import type { GameEvent } from "../../src/domain/events.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectionBoardView,
  type ProjectionState,
} from "../../src/board/projection.ts";
import {
  BOARD_REDUCER_VERSION,
  createBoardProjectionAdapter,
  type BoardProjectionAdapterOptions,
} from "../../src/board/board-projection.ts";
import { readCanonical } from "./command-service.ts";
import { boardStreamId, eventStreamId, nextGeneration } from "./names.ts";
import type { Stores } from "../persistence/stores.ts";

export interface RebuildDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
}

export interface RebuildOptions {
  /** Target generation id. Defaults to the next id after the active one. */
  generation?: string;
  now?: () => number;
  /** Adapter factory (overridable so a corrupt reducer can be exercised in tests). */
  makeAdapter?: (
    options: BoardProjectionAdapterOptions,
  ) => ProjectionAdapter<ProjectionState, GameEvent>;
}

export interface RebuildEquivalence {
  /** The rebuilt board equals the authoritative aggregate fold. */
  boardEqual: boolean;
  /** The rebuilt watermark equals the canonical source head. */
  watermarkEqual: boolean;
}

export interface RebuildResult {
  status: "cutover" | "verification-failed" | "not-found";
  gameId: string;
  fromGeneration: string;
  toGeneration: string;
  /** Canonical source head the rebuild caught up to (null for an empty log). */
  canonicalHead: string | null;
  /** The rebuilt generation's embedded watermark. */
  sourceThroughOffset: string | null;
  /** The durable active generation after this operation. */
  activeGeneration: string;
  equivalence: RebuildEquivalence;
  /** Every generation retained for the game, oldest first. */
  retainedGenerations: string[];
}

/**
 * Rebuild `gameId`'s board into a fresh generation and, if it verifies, cut the
 * durable active pointer over to it. Idempotent-safe to re-run: a re-run targets
 * the next fresh generation id.
 */
export async function rebuildBoardGeneration(
  deps: RebuildDeps,
  gameId: string,
  options: RebuildOptions = {},
): Promise<RebuildResult> {
  const now = options.now ?? (() => Date.now());
  const game = deps.stores.games.get(gameId);
  if (!game) {
    return {
      status: "not-found",
      gameId,
      fromGeneration: "",
      toGeneration: "",
      canonicalHead: null,
      sourceThroughOffset: null,
      activeGeneration: "",
      equivalence: { boardEqual: false, watermarkEqual: false },
      retainedGenerations: [],
    };
  }

  const fromGeneration = game.generation;
  const toGeneration = options.generation ?? nextGeneration(fromGeneration);
  if (toGeneration === fromGeneration) {
    throw new Error(`refusing to rebuild into the active generation "${fromGeneration}"`);
  }
  const outputStreamId = boardStreamId(gameId, toGeneration);

  // Record the new generation as "building" before any output is written.
  deps.stores.generations.put({
    gameId,
    generation: toGeneration,
    streamId: outputStreamId,
    reducerVersion: BOARD_REDUCER_VERSION,
    status: "building",
    sourceThroughOffset: null,
    createdAt: now(),
  });

  const makeAdapter = options.makeAdapter ?? createBoardProjectionAdapter;
  const adapter = makeAdapter({
    gameId,
    sourceStreamId: eventStreamId(gameId),
    outputStreamId,
    generation: toGeneration,
  });
  const runtime = new ProjectionRuntime({
    protocol: deps.protocol,
    adapter,
  });
  await runtime.catchUp();
  const status = await runtime.status();

  // Verify against the authoritative aggregate fold at the canonical head.
  const { events, head } = await readCanonical(deps.protocol, eventStreamId(gameId));
  const authoritative = aggregateBoardView(foldAggregate(events));
  const rebuiltView = projectionBoardView(runtime.currentState());
  const rebuiltWatermark = status.sourceThroughOffset;
  const canonicalHead = events.length > 0 ? head : null;

  const boardEqual = boardsEqual(rebuiltView, authoritative);
  const watermarkEqual = (rebuiltWatermark ?? null) === canonicalHead;
  const equivalence: RebuildEquivalence = { boardEqual, watermarkEqual };

  const base = {
    gameId,
    fromGeneration,
    toGeneration,
    canonicalHead,
    sourceThroughOffset: rebuiltWatermark,
    equivalence,
  };

  if (boardEqual && watermarkEqual) {
    // Cut over: retire old, activate new, repoint the game — one transaction.
    deps.stores.generations.activate(gameId, toGeneration, rebuiltWatermark, now());
    return {
      ...base,
      status: "cutover",
      activeGeneration: toGeneration,
      retainedGenerations: deps.stores.generations.list(gameId).map((g) => g.generation),
    };
  }

  // Verification failed: mark the new generation failed and keep the old active.
  deps.stores.generations.put({
    gameId,
    generation: toGeneration,
    streamId: outputStreamId,
    reducerVersion: BOARD_REDUCER_VERSION,
    status: "failed",
    sourceThroughOffset: rebuiltWatermark,
    createdAt: now(),
  });
  return {
    ...base,
    status: "verification-failed",
    activeGeneration: fromGeneration,
    retainedGenerations: deps.stores.generations.list(gameId).map((g) => g.generation),
  };
}
