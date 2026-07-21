/**
 * Board materialization for `GET /board` and the decision watermark.
 *
 * Wraps the Batch 2 replay-safe {@link ProjectionRuntime}: catches the separate
 * board projection up to the canonical head and returns its state plus the
 * causal `sourceThroughOffset`. Runtimes are cached per game so repeated reads
 * continue from memory instead of re-scanning the projection stream.
 */

import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import type { GameEvent } from "../src/events.ts";
import type { ProjectionState } from "../src/projection.ts";
import { createBoardProjectionAdapter } from "../src/materializer/board-projection.ts";
import { BOARD_GENERATION, boardStreamId, eventStreamId } from "./names.ts";

export type BoardRuntimeCache = Map<string, ProjectionRuntime<ProjectionState, GameEvent>>;

export function createBoardRuntimeCache(): BoardRuntimeCache {
  return new Map();
}

function runtimeFor(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCache,
  gameId: string,
): ProjectionRuntime<ProjectionState, GameEvent> {
  const existing = cache.get(gameId);
  if (existing) return existing;
  const runtime = new ProjectionRuntime({
    protocol,
    adapter: createBoardProjectionAdapter({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      outputStreamId: boardStreamId(gameId),
      generation: BOARD_GENERATION,
    }),
  });
  cache.set(gameId, runtime);
  return runtime;
}

export interface MaterializedBoard {
  state: ProjectionState;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
}

/** Catch the board projection up to the canonical head and return it. */
export async function materializeBoard(
  protocol: StreamProtocolFactory,
  cache: BoardRuntimeCache,
  gameId: string,
): Promise<MaterializedBoard> {
  const runtime = runtimeFor(protocol, cache, gameId);
  const { status } = await runtime.catchUp();
  return {
    state: runtime.currentState(),
    sourceStreamId: eventStreamId(gameId),
    sourceThroughOffset: status.sourceThroughOffset,
  };
}
