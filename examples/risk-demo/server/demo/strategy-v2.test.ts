/**
 * The v2 scripted-bot policy, exercised on hand-built boards.
 *
 * The board that matters most here is the **turtle**: an opponent who never
 * attacks and stacks one unassailable fortress. The bot must reinforce and
 * fortify toward attacks it can actually make instead of accumulating armies
 * against the fortress.
 */

import { describe, expect, it } from "vitest";

import {
  bestAdvantageFrom,
  chooseAttack,
  chooseFortify,
  chooseOccupy,
  chooseReinforce,
  strategyContext,
  weakestEnemyArmies,
  type StrategyMap,
  type StrategyTerritory,
} from "./strategy-v2.ts";

const BOT = "bot";
const HUMAN = "human";

/**
 * The turtled position, in miniature:
 *
 *   a1 (bot, 55) ── h1 (human, 99 fortress) ── h2 (human, 60)
 *    │
 *   a2 (bot, 1) ── h3 (human, 1)
 *
 * `a1` is the most *exposed* country and the least useful one: two enemies, both
 * unbeatable. `a2` faces the only country on the map worth attacking.
 */
const TURTLE_MAP: StrategyMap = {
  territories: [
    { id: "a1", continentId: "c1", adjacentTerritoryIds: ["a2", "h1", "h2"] },
    { id: "a2", continentId: "c1", adjacentTerritoryIds: ["a1", "h3"] },
    { id: "h1", continentId: "c2", adjacentTerritoryIds: ["a1", "h2"] },
    { id: "h2", continentId: "c2", adjacentTerritoryIds: ["a1", "h1"] },
    { id: "h3", continentId: "c2", adjacentTerritoryIds: ["a2"] },
  ],
  continents: [
    { id: "c1", territoryIds: ["a1", "a2"], reinforcementBonus: 2 },
    { id: "c2", territoryIds: ["h1", "h2", "h3"], reinforcementBonus: 2 },
  ],
};

const TURTLE_TERRITORIES: StrategyTerritory[] = [
  { id: "a1", ownerId: BOT, armies: 55 },
  { id: "a2", ownerId: BOT, armies: 1 },
  { id: "h1", ownerId: HUMAN, armies: 99 },
  { id: "h2", ownerId: HUMAN, armies: 60 },
  { id: "h3", ownerId: HUMAN, armies: 1 },
];

const turtle = (overrides: Partial<Record<string, number>> = {}) =>
  strategyContext(
    BOT,
    TURTLE_TERRITORIES.map((territory) => ({
      ...territory,
      armies: overrides[territory.id] ?? territory.armies,
    })),
    TURTLE_MAP,
  );

describe("board reading", () => {
  it("measures a border by the softest enemy it touches", () => {
    const ctx = turtle();
    expect(weakestEnemyArmies(ctx, "a1")).toBe(60);
    expect(weakestEnemyArmies(ctx, "a2")).toBe(1);
  });

  it("reports no attack at all from a country that cannot spare an army", () => {
    const ctx = turtle();
    expect(bestAdvantageFrom(ctx, "a2")).toBe(-Infinity); // one army: none may leave
    expect(bestAdvantageFrom(ctx, "a1")).toBe(55 - 1 - 60);
  });
});

describe("reinforcement against a turtle", () => {
  it("feeds the border that can attack, not the border facing the fortress", () => {
    const chosen = chooseReinforce(turtle(), { territoryIds: ["a1", "a2"], pool: 3 });
    expect(chosen).toEqual({ type: "reinforce", placements: [{ territoryId: "a2", armies: 3 }] });
  });

  it("still builds toward the softer border when this turn's pool is too small", () => {
    // Neither border can attack after one army; the softer one is nonetheless
    // the one worth accumulating on across turns.
    const chosen = chooseReinforce(turtle(), { territoryIds: ["a1", "a2"], pool: 1 });
    expect(chosen).toEqual({ type: "reinforce", placements: [{ territoryId: "a2", armies: 1 }] });
  });

  it("places somewhere legal even with no border at all", () => {
    const ctx = strategyContext(
      BOT,
      [
        { id: "a1", ownerId: BOT, armies: 2 },
        { id: "a2", ownerId: BOT, armies: 2 },
      ],
      {
        territories: [
          { id: "a1", continentId: "c1", adjacentTerritoryIds: ["a2"] },
          { id: "a2", continentId: "c1", adjacentTerritoryIds: ["a1"] },
        ],
        continents: [{ id: "c1", territoryIds: ["a1", "a2"], reinforcementBonus: 2 }],
      },
    );
    expect(chooseReinforce(ctx, { territoryIds: ["a1", "a2"], pool: 4 })).toEqual({
      type: "reinforce",
      placements: [{ territoryId: "a1", armies: 4 }],
    });
  });
});

describe("attack selection", () => {
  it("takes favourable local odds regardless of a larger enemy elsewhere", () => {
    const ctx = turtle({ a2: 4 });
    const chosen = chooseAttack(ctx, {
      choices: [
        { from: "a1", to: "h1", maxAttackerDice: 3 },
        { from: "a1", to: "h2", maxAttackerDice: 3 },
        { from: "a2", to: "h3", maxAttackerDice: 3 },
      ],
    });
    expect(chosen).toEqual({
      type: "declare-attack",
      from: "a2",
      to: "h3",
      attackerDice: 3,
    });
  });

  it("declines when every throw available is unfavourable", () => {
    const ctx = turtle();
    expect(
      chooseAttack(ctx, {
        choices: [
          { from: "a1", to: "h1", maxAttackerDice: 3 },
          { from: "a1", to: "h2", maxAttackerDice: 3 },
        ],
      }),
    ).toBeNull();
  });
});

describe("fortify as the stalemate breaker", () => {
  it("moves a stack off a border it can never attack out of", () => {
    const chosen = chooseFortify(turtle(), {
      choices: [{ from: "a1", reachable: [{ to: "a2", maxArmies: 54 }] }],
    });
    // Three armies stay to hold the fortress border; the rest go where they can
    // actually be spent.
    expect(chosen).toEqual({ type: "fortify", from: "a1", to: "a2", armies: 52 });
  });

  it("prefers an interior garrison over a border stack when both are idle", () => {
    const ctx = strategyContext(
      BOT,
      [{ id: "a0", ownerId: BOT, armies: 6 }, ...TURTLE_TERRITORIES],
      {
        ...TURTLE_MAP,
        territories: [
          { id: "a0", continentId: "c1", adjacentTerritoryIds: ["a1"] },
          { id: "a1", continentId: "c1", adjacentTerritoryIds: ["a0", "a2", "h1", "h2"] },
          ...TURTLE_MAP.territories.filter((t) => t.id !== "a1"),
        ],
      },
    );
    const chosen = chooseFortify(ctx, {
      choices: [
        { from: "a0", reachable: [{ to: "a2", maxArmies: 5 }] },
        { from: "a1", reachable: [{ to: "a2", maxArmies: 54 }] },
      ],
    });
    expect(chosen).toEqual({ type: "fortify", from: "a0", to: "a2", armies: 5 });
  });

  it("does not trade one stuck border for another", () => {
    const ctx = strategyContext(
      BOT,
      [
        { id: "a1", ownerId: BOT, armies: 9 },
        { id: "a2", ownerId: BOT, armies: 2 },
        { id: "h1", ownerId: HUMAN, armies: 20 },
        { id: "h2", ownerId: HUMAN, armies: 30 },
      ],
      {
        territories: [
          { id: "a1", continentId: "c1", adjacentTerritoryIds: ["a2", "h1"] },
          { id: "a2", continentId: "c1", adjacentTerritoryIds: ["a1", "h2"] },
          { id: "h1", continentId: "c2", adjacentTerritoryIds: ["a1"] },
          { id: "h2", continentId: "c2", adjacentTerritoryIds: ["a2"] },
        ],
        continents: [
          { id: "c1", territoryIds: ["a1", "a2"], reinforcementBonus: 2 },
          { id: "c2", territoryIds: ["h1", "h2"], reinforcementBonus: 2 },
        ],
      },
    );
    expect(
      chooseFortify(ctx, { choices: [{ from: "a1", reachable: [{ to: "a2", maxArmies: 8 }] }] }),
    ).toBeNull();
  });

  it("leaves a stack that can attack exactly where it is", () => {
    const ctx = turtle({ a1: 55, h2: 10 });
    expect(
      chooseFortify(ctx, { choices: [{ from: "a1", reachable: [{ to: "a2", maxArmies: 54 }] }] }),
    ).toBeNull();
  });
});

describe("occupation", () => {
  it("moves the minimum into a country with no enemy border left", () => {
    const ctx = strategyContext(
      BOT,
      [
        { id: "a1", ownerId: BOT, armies: 9 },
        { id: "a2", ownerId: BOT, armies: 1 },
      ],
      {
        territories: [
          { id: "a1", continentId: "c1", adjacentTerritoryIds: ["a2"] },
          { id: "a2", continentId: "c1", adjacentTerritoryIds: ["a1"] },
        ],
        continents: [{ id: "c1", territoryIds: ["a1", "a2"], reinforcementBonus: 2 }],
      },
    );
    expect(
      chooseOccupy(ctx, { attackId: "x", from: "a1", to: "a2", minArmies: 3, maxArmies: 8 }),
    ).toEqual({ type: "occupy-territory", attackId: "x", armies: 3 });
  });

  it("pushes a bounded share forward when the captured country is still exposed", () => {
    const chosen = chooseOccupy(turtle(), {
      attackId: "x",
      from: "a1",
      to: "h1",
      minArmies: 3,
      maxArmies: 9,
    });
    expect(chosen).toEqual({ type: "occupy-territory", attackId: "x", armies: 6 });
  });
});
