import { describe, expect, it } from "bun:test";
import { Schema } from "effect";
import { BoardState } from "./fold.ts";
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
  moveId,
} from "./projection.ts";

describe("Hex Domination board projection reducer", () => {
  for (const [name, events] of [
    ["victory", fullGame],
    ["first 450 events of the four-player game", longGame],
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
        const codec = Schema.fromJsonString(BoardState);
        const state = { ordinal: index + 1, board };
        expect(Schema.decodeSync(codec)(Schema.encodeSync(codec)(state))).toStrictEqual(state);
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
  it("carries the pending defence interrupt and occupation bounds through capture", () => {
    let board = initialProjection("game");
    let declarations = 0;
    let captures = 0;
    for (const [index, event] of fullGame.entries()) {
      board = projectEvent(board, event, String(index), index);
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
        const declaration = fullGame.find(
          (declared) => declared.type === "AttackDeclared" && declared.attackId === event.attackId,
        );
        if (declaration?.type !== "AttackDeclared") throw new Error("capture has no declaration");
        expect(board.combat).toMatchObject({
          status: "awaiting-occupation",
          territoryCaptured: true,
          minArmies: declaration.attackerDice,
          maxArmies: board.territories.find((t) => t.id === event.from)!.armies - 1,
        });
      }
      if (event.type === "TerritoryOccupied") {
        expect(board.combat).toBeNull();
        expect(board.turn?.captures).toBe(captures);
        expect(board.territories.find((t) => t.id === event.to)?.ownerId).toBe(event.playerId);
      }
    }
    expect(declarations).toBeGreaterThan(0);
    expect(captures).toBeGreaterThan(0);
  });

  it("projects the canonical map snapshot verbatim, including label anchors", () => {
    const event = longGame.find((candidate) => candidate.type === "GameStarted")!;
    const board = projectEvent(initialProjection("game"), event, "0", 0);
    expect(board.hexes).toEqual([...event.map.tiles]);
    expect(board.territories.map((t) => t.adjacentTerritoryIds)).toEqual(
      event.map.territories.map((t) => [...t.adjacentTerritoryIds]),
    );
    expect(board.territories.map((t) => t.labelAnchor)).toEqual(
      event.map.territories.map((t) => t.labelAnchor),
    );
    expect(board.continents.map((c) => c.territoryIds)).toEqual(
      event.map.continents.map((c) => [...c.territoryIds]),
    );
  });

  it("tracks the current-turn reinforcement breakdown through placement and reset", () => {
    let board = initialProjection("game");
    let placements = 0;
    let resets = 0;
    for (const [index, event] of longGame.entries()) {
      const previous = board.turn;
      board = projectEvent(board, event, String(index), index);
      const turn = board.turn;
      if (!turn) continue;
      expect(turn.reinforcement.total).toBe(
        turn.reinforcement.base +
          turn.reinforcement.continents.reduce((sum, c) => sum + c.bonus, 0),
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
    const board = fullGame.reduce(
      (state, event, index) => projectEvent(state, event, "one-boundary", index),
      initialProjection("game"),
    );
    expect(board.moves.map((move) => move.id)).toEqual(fullGame.map((_, i) => moveId(i)));
    const events = fullGame.filter((event) => event.commandId === "c-10");
    expect(events.map((event) => event.type)).toEqual([
      "TerritoryOccupied",
      "PlayerEliminated",
      "GameWon",
    ]);
    const moves = board.moves.filter((move) => move.commandId === "c-10");
    expect(new Set(moves.map((move) => move.id)).size).toBe(3);
    expect(
      moves.map(({ kind, commandId, playerId, sourceOffset }) => ({
        kind,
        commandId,
        playerId,
        sourceOffset,
      })),
    ).toEqual(
      events.map((event) => ({
        kind: event.type,
        commandId: event.commandId,
        playerId: "playerId" in event ? event.playerId : undefined,
        sourceOffset: "one-boundary",
      })),
    );
  });

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
