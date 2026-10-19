import { describe, expect, it } from "vitest";
import { Array, Option, Result, Schema } from "effect";
import { BoardState } from "../../src/board/fold.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import { fullGame, longGame } from "../fixtures.ts";
import {
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectEvent,
  projectionBoardView,
  MOVE_FEED_LIMIT,
  moveId,
} from "../../src/board/projection.ts";

describe("Hex Domination board projection reducer", () => {
  for (const [name, events] of [
    ["victory", fullGame],
    ["first 450 events of the four-player game", longGame],
  ] as const) {
    it(`agrees with the aggregate at every prefix: ${name}`, () => {
      let board = initialProjection("game");

      for (const [index, event] of events.entries()) {
        board = Result.getOrThrow(projectEvent(board, event, String(index), index));
        expect(
          boardsEqual(
            projectionBoardView(board),
            aggregateBoardView(foldAggregate(events.slice(0, index + 1))),
          ),
        ).toBe(true);
        const codec = Schema.fromJsonString(BoardState);
        const state = { ordinal: index + 1, board };
        expect(Schema.decodeSync(codec)(Schema.encodeSync(codec)(state))).toStrictEqual(state);
        expect(board.moves.length).toBeLessThanOrEqual(MOVE_FEED_LIMIT);

        if (event.type === "GameStarted") {
          expect(board.hexes).toEqual([...event.map.tiles]);
          expect(Array.map(board.territories, (t) => t.adjacentTerritoryIds)).toEqual(
            Array.map(event.map.territories, (t) => [...t.adjacentTerritoryIds]),
          );
          expect(Array.map(board.territories, (t) => t.labelAnchor)).toEqual(
            Array.map(event.map.territories, (t) => t.labelAnchor),
          );
          expect(Array.map(board.continents, (c) => c.territoryIds)).toEqual(
            Array.map(event.map.continents, (c) => [...c.territoryIds]),
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

  it("carries the pending defence interrupt and occupation bounds through capture", () => {
    let board = initialProjection("game");
    let declarations = 0;
    let captures = 0;

    for (const [index, event] of fullGame.entries()) {
      board = Result.getOrThrow(projectEvent(board, event, String(index), index));

      if (event.type === "AttackDeclared") {
        declarations += 1;
        expect(board.turn?.attacksDeclared).toBe(declarations);
        expect(board.combat).toMatchObject({
          attackId: event.attackId,
          status: "awaiting-defense",
          attackerRolls: event.attackerRolls,
        });
      }

      if (event.type === "AttackResolved" && event.territoryCaptured) {
        captures += 1;

        const declaration = Option.getOrUndefined(
          Array.findFirst(
            fullGame,
            (declared) =>
              declared.type === "AttackDeclared" && declared.attackId === event.attackId,
          ),
        );

        if (declaration?.type !== "AttackDeclared") throw new Error("capture has no declaration");
        expect(board.combat).toMatchObject({
          status: "awaiting-occupation",
          territoryCaptured: true,
          minArmies: declaration.attackerDice,
          maxArmies:
            Option.getOrUndefined(Array.findFirst(board.territories, (t) => t.id === event.from))!
              .armies - 1,
        });
      }

      if (event.type === "TerritoryOccupied") {
        expect(board.combat).toBeNull();
        expect(board.turn?.captures).toBe(captures);
        expect(
          Option.getOrUndefined(Array.findFirst(board.territories, (t) => t.id === event.to))
            ?.ownerId,
        ).toBe(event.playerId);
      }
    }

    expect(declarations).toBeGreaterThan(0);
    expect(captures).toBeGreaterThan(0);
  });

  it("tracks the current-turn reinforcement breakdown through placement and reset", () => {
    let board = initialProjection("game");
    let placements = 0;
    let resets = 0;

    for (const [index, event] of longGame.entries()) {
      const previous = board.turn;
      board = Result.getOrThrow(projectEvent(board, event, String(index), index));
      const turn = board.turn;

      if (!turn) continue;
      expect(turn.reinforcement.total).toBe(
        turn.reinforcement.base +
          Array.reduce(turn.reinforcement.continents, 0, (sum, c) => sum + c.bonus),
      );

      if (event.type === "ArmiesReinforced") {
        placements += 1;
        expect(turn.reinforcementsPlaced).toBe(turn.reinforcement.total);
        expect(turn.reinforcement.remaining).toBe(0);
        expect(turn.phase).toBe("attack");
      }

      if (event.type === "TurnEnded") {
        resets += 1;
        expect(turn.turnId).not.toBe(previous?.turnId);
        expect(turn.reinforcementsPlaced).toBe(0);
        expect(turn.reinforcement.remaining).toBe(turn.reinforcement.total);
      }
    }

    expect(placements).toBeGreaterThan(0);
    expect(resets).toBeGreaterThan(0);
  });

  it("gives each event of a multi-event command a distinct move key", () => {
    const board = Array.reduce(fullGame, initialProjection("game"), (state, event, index) =>
      Result.getOrThrow(projectEvent(state, event, "one-boundary", index)),
    );

    expect(Array.map(board.moves, (move) => move.id)).toEqual(
      Array.map(fullGame, (_, i) => moveId(i)),
    );
    const events = Array.filter(fullGame, (event) => event.commandId === "c-10");
    expect(Array.map(events, (event) => event.type)).toEqual([
      "TerritoryOccupied",
      "PlayerEliminated",
      "GameWon",
    ]);
    const moves = Array.filter(board.moves, (move) => move.commandId === "c-10");
    expect(new Set(Array.map(moves, (move) => move.id)).size).toBe(3);
    expect(
      Array.map(moves, ({ kind, commandId, playerId, sourceOffset }) => ({
        kind,
        commandId,
        playerId,
        sourceOffset,
      })),
    ).toEqual(
      Array.map(events, (event) => ({
        kind: event.type,
        commandId: event.commandId,
        playerId: "playerId" in event ? event.playerId : undefined,
        sourceOffset: "one-boundary",
      })),
    );
  });
});
