import { Schema } from "effect";
import { StreamRef } from "@streamsy/core";
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
  Projection.make({
    id: "hex-board",
    params: { gameId },
    input: StreamRef.json(`games/${gameId}/events`, { schema: GameEvent }),
    process: Projection.fold(BoardState, initialBoard(gameId), (state, { item }, unit) =>
      stepBoard(state, item, unit.ranges.input.nextOffset),
    ),
  });
