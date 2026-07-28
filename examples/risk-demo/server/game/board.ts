/**
 * Board materialization for `GET /board` and the decision watermark.
 *
 * Wraps the replay-safe {@link ProjectionRuntime}: catches the separate
 * board projection up to the canonical head and returns its state plus the
 * causal `sourceThroughOffset`. Runtimes are cached per game so repeated reads
 * continue from memory instead of re-scanning the projection stream.
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

/** Cache runtimes per (game, generation) so a cutover transparently switches streams. */
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
 * Catch the active board projection up to the canonical head and return it. The
 * `generation` names which projection stream is live; after a cutover, callers
 * pass the new generation and a fresh runtime is used, leaving the old stream
 * intact.
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
