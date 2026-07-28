import { describe, expect, it } from "vitest";

import type { CombatView } from "./combat-view.ts";
import {
  attackAgainAction,
  attackTerritoryIds,
  fortifyAction,
  shouldDismissAttackSummary,
  startsAttackSelection,
  type DeclareAttackAction,
} from "./attack-phase.ts";

const ACTION: DeclareAttackAction = {
  type: "declare-attack",
  choices: [
    { from: "a", to: "x", maxAttackerDice: 3 },
    { from: "a", to: "y", maxAttackerDice: 2 },
    { from: "b", to: "z", maxAttackerDice: 1 },
  ],
  submit: {
    type: "declare-attack",
    from: "<choice.from>",
    to: "<choice.to>",
    attackerDice: "<1..choice.maxAttackerDice>",
  },
};

const RESOLVED: CombatView = {
  attackId: "attack-1",
  status: "resolved",
  attackerId: "p1",
  defenderId: "p2",
  from: "a",
  to: "x",
  attackerDice: 3,
  attackerRolls: [6, 4, 2],
  defenderDice: 2,
  defenderRolls: [5, 3],
  attackerLosses: 1,
  defenderLosses: 1,
  territoryCaptured: false,
  resolutionSource: "human",
};

describe("attack-phase map affordances", () => {
  it("highlights legal attackers first, then only the selected source and enemy neighbours", () => {
    expect([...attackTerritoryIds(ACTION)]).toEqual(["a", "b"]);
    expect([...attackTerritoryIds(ACTION, "a")]).toEqual(["a", "x", "y"]);
    expect([...attackTerritoryIds(ACTION, "b")]).toEqual(["b", "z"]);
  });

  it("identifies a fresh attacker selection so the previous dice summary can be cleared", () => {
    expect(startsAttackSelection(ACTION, "b")).toBe(true);
    expect(startsAttackSelection(ACTION, "x")).toBe(false);
    expect(shouldDismissAttackSummary(RESOLVED, ACTION, "b")).toBe(true);
    expect(shouldDismissAttackSummary(RESOLVED, ACTION, "x")).toBe(false);
  });
});

describe("attack-phase canonical actions", () => {
  it("uses the existing fortify action when ending attack with a fortification", () => {
    expect(fortifyAction("a", "b", 2)).toEqual({
      type: "fortify",
      from: "a",
      to: "b",
      armies: 2,
    });
  });

  it("attacks the same pair again directly using fresh legal dice", () => {
    expect(attackAgainAction(ACTION, RESOLVED)).toEqual({
      type: "declare-attack",
      from: "a",
      to: "x",
      attackerDice: 3,
    });
  });

  it("does not repeat a conquest or a pairing that is no longer legal", () => {
    expect(attackAgainAction(ACTION, { ...RESOLVED, territoryCaptured: true })).toBeNull();
    expect(
      attackAgainAction(
        {
          ...ACTION,
          choices: ACTION.choices.filter((choice) => choice.from !== "a" || choice.to !== "x"),
        },
        RESOLVED,
      ),
    ).toBeNull();
  });
});
