/**
 * Hex Domination board bindings for the generic Durable State projection adapter.
 */
import type { StreamId, StreamProtocolFactory } from "@streamsy/core";
import {
  durableStateProjectionAdapter,
  type ProjectionAdapter,
} from "@streamsy/experimental/projection";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";
import type { DurableStateSchemaMap } from "@streamsy/state";

import type { GameEvent } from "../domain/events.ts";
import { boardProjectionTxId } from "./transaction.ts";
import {
  COMBAT_ROW_KEY,
  TURN_ROW_KEY,
  initialProjection,
  projectEvent,
  type ProjectedCombat,
  type ProjectedContinent,
  type ProjectedGame,
  type ProjectedHex,
  type ProjectedMove,
  type ProjectedPlayer,
  type ProjectedTerritory,
  type ProjectedTurn,
  type ProjectionState,
} from "./projection.ts";

/**
 * Identity of the reduce function, not of the stream it writes into.
 *
 * The two are deliberately separate. A *generation* (`board1`, `board2`, …) names
 * one rebuildable output stream for one game; this names the code that built it,
 * and every generation row records the version it was produced under. The rule
 * that follows is the same one the map generator lives by: **any change to what
 * the reducer emits — new event types handled, new or changed row fields — must
 * bump this**, because a projection stream is a durable artefact and a resumed
 * runtime appends to whatever a previous version already wrote.
 *
 * Bumping does not migrate anything by itself. Nothing gates on the value at
 * runtime; it is the signal an operator reads, and `rebuildBoardGeneration`
 * (`bun run rebuild <game-id>`) is what acts on it, replaying canonical history
 * into a fresh generation built entirely by the current reducer and cutting over
 * only after verification.
 *
 * `board-2` handles `PlayerRenamed` and `PlayerLeft` — which mutate and delete
 * player rows in the lobby — and carries a departing seat's `name` on its move.
 * A `board1` stream written by `board-1` therefore cannot be resumed by this
 * reducer without a rebuild: its history predates those events entirely.
 */
export const BOARD_REDUCER_VERSION = "hex-domination:board-2";

const codec = <T>(): JsonCodec<T> => ({
  encode: (value) => value,
  decode: (value) => value as T,
});
const eventSchema: JsonCodec<GameEvent> = {
  encode: (event) => event,
  decode: (value) => value as GameEvent,
};

const boardSchema = {
  games: { type: "game", primaryKey: "id", schema: codec<ProjectedGame>() },
  players: {
    type: "player",
    primaryKey: "id",
    schema: codec<ProjectedPlayer>(),
  },
  hexes: { type: "hex", primaryKey: "id", schema: codec<ProjectedHex>() },
  territories: { type: "territory", primaryKey: "id", schema: codec<ProjectedTerritory>() },
  continents: { type: "continent", primaryKey: "id", schema: codec<ProjectedContinent>() },
  turn: { type: "turn", primaryKey: "id", schema: codec<ProjectedTurn>() },
  combat: { type: "combat", primaryKey: "id", schema: codec<ProjectedCombat>() },
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
  generation: string;
}

export function createBoardProjectionAdapter(
  options: BoardProjectionAdapterOptions,
): ProjectionAdapter<ProjectionState, GameEvent> {
  return durableStateProjectionAdapter({
    processorId: options.processorId ?? `risk-board:${options.gameId}`,
    generation: options.generation,
    reducerVersion: BOARD_REDUCER_VERSION,
    sourceStreamId: options.sourceStreamId,
    outputStreamId: options.outputStreamId,
    sourceSchema: eventSchema,
    schema: boardSchema,
    initial: () => initialProjection(options.gameId),
    reduce: (state, event, meta) => projectEvent(state, event, meta.sourceThroughOffset),
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
