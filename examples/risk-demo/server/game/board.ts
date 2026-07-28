/**
 * `Hex Domination` board materialization for `GET /board` and the decision
 * watermark.
 *
 * A replay-safe {@link ProjectionRuntime} is cached per game and generation.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import type { GameEvent } from "../../src/domain/events.ts";
import type { ProjectionState } from "../../src/board/projection.ts";
import { createBoardProjectionAdapter } from "../../src/board/board-projection.ts";
import { BOARD_GENERATION, boardStreamId, eventStreamId } from "./names.ts";

export type BoardRuntimeCache = Map<string, ProjectionRuntime<ProjectionState, GameEvent>>;

export function createBoardRuntimeCache(): BoardRuntimeCache {
  return new Map();
}

function cacheKey(gameId: string, generation: string): string {
  return `${gameId}:${generation}`;
}

function runtimeFor(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCache,
  gameId: string,
  generation: string,
): ProjectionRuntime<ProjectionState, GameEvent> {
  const key = cacheKey(gameId, generation);
  const existing = cache.get(key);
  if (existing) return existing;
  const runtime = new ProjectionRuntime({
    protocol,
    adapter: createBoardProjectionAdapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: boardStreamId(gameId, generation),
      generation,
    }),
  });
  cache.set(key, runtime);
  return runtime;
}

export interface MaterializedBoard {
  state: ProjectionState;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
}

/**
 * Catch the active current board projection up to the canonical head and return it,
 * together with the causal watermark the decision resource reports.
 */
export async function materializeBoard(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCache,
  gameId: string,
  generation: string = BOARD_GENERATION,
): Promise<MaterializedBoard> {
  const runtime = runtimeFor(protocol, cache, gameId, generation);
  const { status } = await runtime.catchUp();
  return {
    state: runtime.currentState(),
    sourceStreamId: eventStreamId(gameId),
    sourceThroughOffset: status.sourceThroughOffset,
    generation,
  };
}
