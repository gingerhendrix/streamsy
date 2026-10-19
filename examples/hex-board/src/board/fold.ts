import { Effect, Result, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { GameEvent } from "../domain/events.ts";
import { initialProjection, projectEvent, ProjectionIntegrityError } from "./projection.ts";
import { ProjectionStateSchema } from "./schemas.ts";

/** The next move ordinal survives restart independently of page boundaries. */
export const BoardState = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  board: ProjectionStateSchema,
});

export type BoardState = typeof BoardState.Type;

export const initialBoard = ({ gameId }: { readonly gameId: string }): BoardState => ({
  ordinal: 0,
  board: initialProjection(gameId),
});

export const stepBoard = (
  state: BoardState,
  event: GameEvent,
  through: string,
): Result.Result<BoardState, ProjectionIntegrityError> =>
  Result.map(projectEvent(state.board, event, through, state.ordinal), (board) => ({
    ordinal: state.ordinal + 1,
    board,
  }));

export class GameEvents extends Streamsy.json<GameEvents>()(
  "HexBoard/GameEvents",
  "games/:gameId/events",
  { params: { gameId: Schema.String }, schema: GameEvent },
) {}

export const board = Projection.fold({
  id: "hex-board",
  params: { gameId: Schema.String },
  inputs: { input: GameEvents },
  state: { schema: BoardState, initial: initialBoard },
  fold: (state, { item }, unit) =>
    Effect.fromResult(stepBoard(state, item, unit.ranges.input.nextOffset)),
});
