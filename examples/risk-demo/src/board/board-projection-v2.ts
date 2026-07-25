/**
 * `risk-demo-v2` board bindings for the generic Durable State projection adapter.
 *
 * A separate reducer version *and* a separate generation id from v1: a v2 game's
 * board is a new projection generation, never a mutation of an existing one, so
 * v1 games keep projecting on the v1 generation byte-for-byte.
 */
import type { StreamId, StreamProtocolFactory } from "@streamsy/core";
import {
  durableStateProjectionAdapter,
  type ProjectionAdapter,
} from "@streamsy/experimental/projection";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";
import type { DurableStateSchemaMap } from "@streamsy/state";

import {
  normalizeGameEventV2,
  type GameEventV2,
  type PlayerController,
} from "../domain/events-v2.ts";
import { RULESET_V2 } from "../domain/map-v2.ts";
import { boardProjectionTxId } from "./transaction.ts";
import {
  COMBAT_ROW_KEY,
  TURN_ROW_KEY,
  initialProjectionV2,
  projectEventV2,
  type ProjectedCombatV2,
  type ProjectedContinentV2,
  type ProjectedGameV2,
  type ProjectedHexV2,
  type ProjectedMoveV2,
  type ProjectedPlayerV2,
  type ProjectedTerritoryV2,
  type ProjectedTurnV2,
  type ProjectionStateV2,
} from "./projection-v2.ts";

export const BOARD_REDUCER_VERSION_V2 = `${RULESET_V2}:board-1`;

const codec = <T>(): JsonCodec<T> => ({
  encode: (value) => value,
  decode: (value) => value as T,
});
const eventSchemaV2: JsonCodec<GameEventV2> = {
  encode: (event) => event,
  decode: normalizeGameEventV2,
};

function normalizePlayer(value: unknown): ProjectedPlayerV2 {
  const player = value as Omit<ProjectedPlayerV2, "controller"> & {
    controller: PlayerController | "agent";
  };
  return player.controller === "agent"
    ? ({ ...player, controller: "bot" } as ProjectedPlayerV2)
    : (player as ProjectedPlayerV2);
}

function normalizeProjectionState(value: unknown): ProjectionStateV2 {
  const state = value as ProjectionStateV2;
  return { ...state, players: state.players.map(normalizePlayer) };
}

const boardSchemaV2 = {
  games: { type: "game", primaryKey: "id", schema: codec<ProjectedGameV2>() },
  players: {
    type: "player",
    primaryKey: "id",
    schema: { encode: (value) => value, decode: normalizePlayer },
  },
  hexes: { type: "hex", primaryKey: "id", schema: codec<ProjectedHexV2>() },
  territories: { type: "territory", primaryKey: "id", schema: codec<ProjectedTerritoryV2>() },
  continents: { type: "continent", primaryKey: "id", schema: codec<ProjectedContinentV2>() },
  turn: { type: "turn", primaryKey: "id", schema: codec<ProjectedTurnV2>() },
  combat: { type: "combat", primaryKey: "id", schema: codec<ProjectedCombatV2>() },
  moves: { type: "move", primaryKey: "id", schema: codec<ProjectedMoveV2>() },
  projectionMeta: {
    primaryKey: () => "board",
    schema: {
      encode: (value: { snapshot: ProjectionStateV2 }) => value,
      decode: (value) => {
        const meta = value as { snapshot: ProjectionStateV2 };
        return { snapshot: normalizeProjectionState(meta.snapshot) };
      },
    },
  },
} satisfies DurableStateSchemaMap;

export interface BoardProjectionAdapterOptionsV2 {
  gameId: string;
  sourceStreamId: StreamId;
  outputStreamId: StreamId;
  processorId?: string;
  generation: string;
}

export function createBoardProjectionAdapterV2(
  options: BoardProjectionAdapterOptionsV2,
): ProjectionAdapter<ProjectionStateV2, GameEventV2> {
  return durableStateProjectionAdapter({
    processorId: options.processorId ?? `risk-board-v2:${options.gameId}`,
    generation: options.generation,
    reducerVersion: BOARD_REDUCER_VERSION_V2,
    sourceStreamId: options.sourceStreamId,
    outputStreamId: options.outputStreamId,
    sourceSchema: eventSchemaV2,
    schema: boardSchemaV2,
    initial: () => initialProjectionV2(options.gameId),
    reduce: (state, event, meta) => projectEventV2(state, event, meta.sourceThroughOffset),
    txid: (event, meta) => boardProjectionTxId(event.commandId, meta.sourceThroughOffset),
    // `turn` and `combat` are zero-or-one collections: omitting the row is what
    // makes the adapter emit a delete, which is how a resolved combat clears.
    rows: (state) => [
      { type: "game", key: state.game.id || options.gameId, value: state.game },
      ...state.players.map((value) => ({ type: "player", key: value.id, value })),
      ...state.hexes.map((value) => ({ type: "hex", key: value.id, value })),
      ...state.territories.map((value) => ({ type: "territory", key: value.id, value })),
      ...state.continents.map((value) => ({ type: "continent", key: value.id, value })),
      ...(state.turn ? [{ type: "turn", key: TURN_ROW_KEY, value: state.turn }] : []),
      ...(state.combat ? [{ type: "combat", key: COMBAT_ROW_KEY, value: state.combat }] : []),
      ...state.moves.map((value) => ({ type: "move", key: value.id, value })),
    ],
    meta: { type: "projectionMeta", key: "board" },
  });
}

export async function writeCanonicalEventsV2(
  protocol: StreamProtocolFactory,
  streamId: StreamId,
  events: readonly GameEventV2[],
): Promise<string[]> {
  const stream = await createJsonProtocol(protocol, eventSchemaV2).getOrCreate(streamId);
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
