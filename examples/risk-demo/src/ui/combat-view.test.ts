import { describe, expect, it } from "vitest";

import type {
  ProjectedCombatV2,
  ProjectedMoveV2,
  ProjectedTurnV2,
} from "../board/projection-v2.ts";
import { combatView } from "./combat-view.ts";

const PENDING: ProjectedCombatV2 = {
  id: "combat",
  attackId: "cmd-42",
  turnId: "round-2:p1",
  status: "awaiting-defense",
  attackerId: "p1",
  defenderId: "p2",
  from: "t1",
  to: "t2",
  attackerDice: 3,
  attackerRolls: [6, 5, 2],
  defenderDice: 2,
  declaredAt: 1_000,
  defenseDeadlineAt: 16_000,
};

const TURN: ProjectedTurnV2 = {
  id: "turn",
  turnId: "round-2:p1",
  round: 2,
  playerId: "p1",
  phase: "attack",
  reinforcement: { base: 3, continents: [], total: 3, remaining: 0 },
  reinforcementsPlaced: 3,
  attacksDeclared: 1,
  throwsResolved: 1,
  captures: 0,
  eliminations: 0,
  latestDice: {
    attackId: "cmd-42",
    from: "t1",
    to: "t2",
    attackerRolls: [6, 5, 2],
    defenderRolls: [4, 3],
    attackerLosses: 0,
    defenderLosses: 2,
    territoryCaptured: false,
    resolutionSource: "timeout",
  },
};

const MOVES: ProjectedMoveV2[] = [
  {
    id: "9",
    commandId: "c9",
    kind: "AttackResolved",
    playerId: "p2",
    sourceOffset: "9",
    attackId: "cmd-42",
  },
  {
    id: "8",
    commandId: "cmd-42",
    kind: "AttackDeclared",
    playerId: "p1",
    sourceOffset: "8",
    attackId: "cmd-42",
  },
];

describe("combat view", () => {
  it("prefers the live combat row while an attack is open", () => {
    const view = combatView({ combat: PENDING, turn: TURN, moves: MOVES });
    expect(view).toMatchObject({
      attackId: "cmd-42",
      status: "awaiting-defense",
      attackerId: "p1",
      defenderId: "p2",
      attackerRolls: [6, 5, 2],
      defenderDice: 2,
      defenseDeadlineAt: 16_000,
      declaredAt: 1_000,
    });
    // Nothing is invented for a roll that has not happened yet.
    expect(view?.defenderRolls).toBeUndefined();
  });

  it("carries the occupation bounds while a capture waits to be occupied", () => {
    const view = combatView({
      combat: {
        ...PENDING,
        status: "awaiting-occupation",
        defenderRolls: [2, 1],
        attackerLosses: 0,
        defenderLosses: 2,
        territoryCaptured: true,
        resolutionSource: "human",
        minArmies: 3,
        maxArmies: 5,
      },
      turn: TURN,
      moves: MOVES,
    });
    expect(view).toMatchObject({ status: "awaiting-occupation", minArmies: 3, maxArmies: 5 });
    // A closed combat has no live deadline to count down.
    expect(view?.defenseDeadlineAt).toBeUndefined();
  });

  it("falls back to the turn row once the projection clears combat", () => {
    const view = combatView({ combat: null, turn: TURN, moves: MOVES });
    expect(view).toMatchObject({
      attackId: "cmd-42",
      status: "resolved",
      attackerId: "p1",
      defenderId: "p2",
      defenderRolls: [4, 3],
      defenderLosses: 2,
      resolutionSource: "timeout",
    });
  });

  it("still names the attacker when the move feed has scrolled past the throw", () => {
    const view = combatView({ combat: null, turn: TURN, moves: [] });
    expect(view?.attackerId).toBe("p1");
    expect(view?.defenderId).toBeUndefined();
  });

  it("has nothing to show before the first throw of a turn", () => {
    expect(combatView({ combat: null, turn: { ...TURN, latestDice: undefined }, moves: [] })).toBe(
      null,
    );
    expect(combatView({ combat: null, turn: null, moves: [] })).toBe(null);
  });
});
