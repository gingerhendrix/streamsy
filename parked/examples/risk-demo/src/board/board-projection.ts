/**
 * Hex Domination board vocabulary: the row set a projection state implies, the
 * application's own checkpoint row, and the reducer's identity.
 *
 * The mesh wiring that turns these into a running projection lives in
 * `./mesh.ts`.
 */
import type { StreamId, StreamProtocolFactory } from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "@streamsy/core/json";

import { GameEvent, type GameEvent as GameEventType } from "../domain/events.ts";
import { Schema } from "effect";
import { COMBAT_ROW_KEY, TURN_ROW_KEY, type ProjectionState } from "./projection.ts";

/** One Durable State row emitted by the board projection. */
export interface DurableStateProjectionRow {
  type: string;
  key: string;
  value: unknown;
}

/** The application's own checkpoint row: a complete state snapshot plus context. */
export interface BoardProjectionMetaRow {
  sourceStreamId: string;
  sourceThroughOffset: string;
  /** 0-based ordinal of the last applied canonical event. */
  sourceSeq: number;
  generation: string;
  reducerVersion: string;
  snapshot: ProjectionState;
}

export const BOARD_META_TYPE = "projectionMeta";
export const BOARD_META_KEY = "board";

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
 *
 * `board-3` is the mesh cutover, and it changes the *output format*, not just
 * what the reducer understands. Two things moved at once:
 *
 * - every transaction now carries a reserved `__streamsy.mesh.lineage.v1` row,
 *   and mesh recovery refuses a non-empty stream that does not end at one; and
 * - move rows are keyed by canonical event ordinal instead of by source offset,
 *   because a delivery boundary gives every event in it the same offset.
 *
 * A `board-2` stream has neither. It is not resumable by this reducer under any
 * circumstances, and — unlike the `board-1` case — it fails loudly rather than
 * merely producing stale rows: recovery reports `incompatible-output` and the
 * board read raises. The repair is the same one this version scheme exists to
 * signal: `bun run rebuild <game-id>`, which replays canonical history into a
 * fresh generation, verifies it against the aggregate fold, and only then cuts
 * over. Canonical history is untouched by any of this, so nothing is lost.
 */
export const BOARD_REDUCER_VERSION = "hex-domination:board-3";

const eventSchema: JsonCodec<GameEventType> = {
  encode: (event) => event,
  decode: Schema.decodeUnknownSync(GameEvent),
};

/**
 * The complete row set a projection state implies.
 *
 * The collections themselves are declared once, in the browser mirror's typed
 * schema (`src/ui/board-stream-db.ts`); this is the single place that decides
 * which rows a state puts in them, and `board-stream-db.test.ts` checks the two
 * agree.
 *
 * `turn` and `combat` are zero-or-one collections: omitting the row is what
 * makes the row diff emit a delete, which is how a resolved combat clears.
 */
export function boardRows(
  state: ProjectionState,
  gameId = "",
): readonly DurableStateProjectionRow[] {
  return [
    { type: "game", key: state.game.id || gameId, value: state.game },
    ...state.players.map((value) => ({ type: "player", key: value.id, value })),
    ...state.hexes.map((value) => ({ type: "hex", key: value.id, value })),
    ...state.territories.map((value) => ({ type: "territory", key: value.id, value })),
    ...state.continents.map((value) => ({ type: "continent", key: value.id, value })),
    ...(state.turn ? [{ type: "turn", key: TURN_ROW_KEY, value: state.turn }] : []),
    ...(state.combat ? [{ type: "combat", key: COMBAT_ROW_KEY, value: state.combat }] : []),
    ...state.moves.map((value) => ({ type: "move", key: value.id, value })),
  ];
}

// oxlint-disable-next-line effecttsgo/async-function -- StreamProtocolFactory is a Promise-native compatibility API and this exported helper preserves that contract.
export async function writeCanonicalEvents(
  protocol: StreamProtocolFactory,
  streamId: StreamId,
  events: readonly GameEventType[],
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
