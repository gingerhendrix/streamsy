/** Risk board bindings for the generic Durable State projection adapter. */
import type { StreamId, StreamProtocolFactory } from "@streamsy/core";
import {
  durableStateProjectionAdapter,
  type ProjectionAdapter,
} from "@streamsy/experimental/projection";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";
import type { DurableStateSchemaMap } from "@streamsy/state";

import type { GameEvent } from "../events.ts";
import { RULESET } from "../map.ts";
import {
  initialProjection,
  projectEvent,
  type ProjectedGame,
  type ProjectedPlayer,
  type ProjectedMove,
  type ProjectedTerritory,
  type ProjectionState,
} from "../projection.ts";

export const BOARD_REDUCER_VERSION = `${RULESET}:board-1`;

const codec = <T>(): JsonCodec<T> => ({
  encode: (value) => value,
  decode: (value) => value as T,
});
const eventSchema = codec<GameEvent>();

const boardSchema = {
  games: { type: "game", primaryKey: "id", schema: codec<ProjectedGame>() },
  players: { type: "player", primaryKey: "id", schema: codec<ProjectedPlayer>() },
  territories: {
    type: "territory",
    primaryKey: "id",
    schema: codec<ProjectedTerritory>(),
  },
  moves: { type: "move", primaryKey: "id", schema: codec<ProjectedMove>() },
  projectionMeta: {
    primaryKey: () => "board",
    schema: codec<{ snapshot: ProjectionState }>(),
  },
} satisfies DurableStateSchemaMap;

export interface BoardProjectionAdapterOptions {
  gameId: string;
  sourceStreamId: StreamId;
  outputStreamId: StreamId;
  processorId?: string;
  generation?: string;
}

export function createBoardProjectionAdapter(
  options: BoardProjectionAdapterOptions,
): ProjectionAdapter<ProjectionState, GameEvent> {
  return durableStateProjectionAdapter({
    processorId: options.processorId ?? `risk-board:${options.gameId}`,
    generation: options.generation ?? "v1",
    reducerVersion: BOARD_REDUCER_VERSION,
    sourceStreamId: options.sourceStreamId,
    outputStreamId: options.outputStreamId,
    sourceSchema: eventSchema,
    schema: boardSchema,
    initial: initialProjection,
    reduce: (state, event, meta) => projectEvent(state, event, meta.sourceThroughOffset),
    rows: (state) => [
      { type: "game", key: state.game.id ?? options.gameId, value: state.game },
      ...state.players.map((value) => ({ type: "player", key: value.id, value })),
      ...state.territories.map((value) => ({ type: "territory", key: value.id, value })),
      ...state.moves.map((value) => ({ type: "move", key: value.id, value })),
    ],
    meta: { type: "projectionMeta", key: "board" },
  });
}

export async function writeCanonicalEvents(
  protocol: StreamProtocolFactory,
  streamId: StreamId,
  events: readonly GameEvent[],
): Promise<string[]> {
  const stream = await createJsonProtocol(protocol, eventSchema).getOrCreate(streamId);
  const offsets: string[] = [];
  for (const event of events) {
    const appended = await stream.append(event);
    if (appended.status !== "appended") {
      throw new Error(`cannot append canonical event: ${appended.status}`);
    }
    offsets.push(appended.offset);
  }
  return offsets;
}
