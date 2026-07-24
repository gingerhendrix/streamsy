/**
 * `risk-demo-v2` board materialization for `GET /board` and the decision
 * watermark.
 *
 * Structurally identical to the v1 {@link ../game/board.ts} materializer — the
 * same replay-safe {@link ProjectionRuntime}, the same per-(game, generation)
 * runtime cache — but bound to the v2 reducer and its own generation lineage, so
 * the two never share a projection stream or a schema.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import type { GameEventV2 } from "../../src/domain/events-v2.ts";
import type { ProjectionStateV2 } from "../../src/board/projection-v2.ts";
import { createBoardProjectionAdapterV2 } from "../../src/board/board-projection-v2.ts";
import { BOARD_GENERATION_V2, boardStreamId, eventStreamId } from "./names.ts";

export type BoardRuntimeCacheV2 = Map<string, ProjectionRuntime<ProjectionStateV2, GameEventV2>>;

export function createBoardRuntimeCacheV2(): BoardRuntimeCacheV2 {
  return new Map();
}

function cacheKey(gameId: string, generation: string): string {
  return `${gameId}:${generation}`;
}

function runtimeFor(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCacheV2,
  gameId: string,
  generation: string,
): ProjectionRuntime<ProjectionStateV2, GameEventV2> {
  const key = cacheKey(gameId, generation);
  const existing = cache.get(key);
  if (existing) return existing;
  const runtime = new ProjectionRuntime({
    protocol,
    adapter: createBoardProjectionAdapterV2({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: boardStreamId(gameId, generation),
      generation,
    }),
  });
  cache.set(key, runtime);
  return runtime;
}

export interface MaterializedBoardV2 {
  state: ProjectionStateV2;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
}

/**
 * Catch the active v2 board projection up to the canonical head and return it,
 * together with the causal watermark the decision resource reports.
 */
export async function materializeBoardV2(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCacheV2,
  gameId: string,
  generation: string = BOARD_GENERATION_V2,
): Promise<MaterializedBoardV2> {
  const runtime = runtimeFor(protocol, cache, gameId, generation);
  const { status } = await runtime.catchUp();
  return {
    state: runtime.currentState(),
    sourceStreamId: eventStreamId(gameId),
    sourceThroughOffset: status.sourceThroughOffset,
    generation,
  };
}
