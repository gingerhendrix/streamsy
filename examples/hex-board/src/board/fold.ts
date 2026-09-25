import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { GameEvent } from "../domain/events.ts";
import { initialProjection, projectEvent } from "./projection.ts";
import { ProjectionStateSchema } from "./schemas.ts";

/** The next move ordinal survives restart independently of page boundaries. */
export const BoardState = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  board: ProjectionStateSchema,
});
export type BoardState = typeof BoardState.Type;

export const initialBoard = (gameId: string): BoardState => ({
  ordinal: 0,
  board: initialProjection(gameId),
});

export const stepBoard = (state: BoardState, event: GameEvent, through: string): BoardState => ({
  ordinal: state.ordinal + 1,
  board: projectEvent(state.board, event, through, state.ordinal),
});

export const boardProjection = (gameId: string) =>
  Projection.fused({
    id: "hex-board",
    params: { gameId },
    inputs: { input: GameEvents.at({ gameId }) },
    process: Projection.fold(BoardState, initialBoard(gameId), (state, { item }, unit) =>
      stepBoard(state, item, unit.ranges.input.nextOffset),
    ),
  });

export class GameEvents extends Streamsy.json<GameEvents>()(
  "HexBoard/GameEvents",
  "games/:gameId/events",
  { params: { gameId: Schema.String }, schema: GameEvent },
) {}
