import { describe, expect, it } from "bun:test";
import { foldAggregate } from "../domain/aggregate.ts";
import { fullGame, longGame } from "../../test/fixtures.ts";
import {
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectEvent,
  projectionBoardView,
  ProjectionIntegrityError,
  MOVE_FEED_LIMIT,
} from "./projection.ts";

describe("Hex Domination board projection reducer", () => {
  for (const [name, events] of [
    ["victory", fullGame],
    ["long four-player game", longGame],
  ] as const) {
    it(`agrees with the aggregate at every prefix: ${name}`, () => {
      let board = initialProjection("game");
      for (const [index, event] of events.entries()) {
        board = projectEvent(board, event, String(index), index);
        expect(
          boardsEqual(
            projectionBoardView(board),
            aggregateBoardView(foldAggregate(events.slice(0, index + 1))),
          ),
        ).toBe(true);
        expect(board.moves.length).toBeLessThanOrEqual(MOVE_FEED_LIMIT);
        if (event.type === "GameStarted") {
          expect(board.hexes).toEqual([...event.map.tiles]);
          expect(board.territories.map((t) => t.adjacentTerritoryIds)).toEqual(
            event.map.territories.map((t) => [...t.adjacentTerritoryIds]),
          );
        }
        if (event.type === "AttackDeclared") {
          expect(board.combat).toMatchObject({
            status: "awaiting-defense",
            attackerRolls: event.attackerRolls,
          });
        }
        if (event.type === "AttackResolved" && event.territoryCaptured) {
          expect(board.combat?.status).toBe("awaiting-occupation");
        }
        if (event.type === "TerritoryOccupied") expect(board.combat).toBeNull();
        if (event.type === "TurnEnded") {
          expect(board.turn?.reinforcementsPlaced).toBe(0);
          expect(board.turn?.attacksDeclared).toBe(0);
        }
      }
      if (name === "victory") expect(board.game.status).toBe("finished");
    });
  }
  it("rejects a resolution that contradicts its declaration", () => {
    let board = initialProjection("game");
    for (const [index, event] of fullGame.entries()) {
      if (event.type === "AttackResolved") {
        expect(() =>
          projectEvent(board, { ...event, attackerRolls: [1, 1, 1] }, String(index), index),
        ).toThrow(ProjectionIntegrityError);
        return;
      }
      board = projectEvent(board, event, String(index), index);
    }
    throw new Error("fixture has no resolution");
  });
});
