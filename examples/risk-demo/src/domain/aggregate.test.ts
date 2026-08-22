import { describe, expect, it } from "vitest";
/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */

import {
  AggregateIntegrityError,
  computeReinforcement,
  controlledContinents,
  foldAggregate,
  friendlyReachable,
  nextTurn,
  ownedBy,
} from "./aggregate.ts";
import type { AggregateState } from "./aggregate.ts";
import { compareRolls, legalDefenderDice, maxAttackerDice } from "./dice.ts";
import type { GameEvent } from "./events.ts";
import { RULES, baseReinforcement, continentBonus } from "./map.ts";
import {
  armForAttack,
  declareAttack,
  nextCommandId,
  occupyPending,
  placeAllReinforcements,
  startGame,
  throwUntilCapture,
  type ScriptedGame,
} from "../../test/testkit.ts";

// ---------------------------------------------------------------------------
// Dice comparison
// ---------------------------------------------------------------------------

describe("current dice comparison", () => {
  it("sorts descending and compares pairwise, with ties favouring the defender", () => {
    expect(compareRolls([6, 5, 2], [6, 4])).toEqual({ attackerLosses: 1, defenderLosses: 1 });
    expect(compareRolls([2, 3, 1], [6, 5])).toEqual({ attackerLosses: 2, defenderLosses: 0 });
    expect(compareRolls([6, 6], [1, 1])).toEqual({ attackerLosses: 0, defenderLosses: 2 });
  });

  it("is order-insensitive: unsorted input compares identically", () => {
    expect(compareRolls([2, 6, 5], [4, 6])).toEqual(compareRolls([6, 5, 2], [6, 4]));
  });

  it("compares only min(attacker, defender) pairs", () => {
    expect(compareRolls([6, 6, 6], [1])).toEqual({ attackerLosses: 0, defenderLosses: 1 });
    expect(compareRolls([6], [1, 1])).toEqual({ attackerLosses: 0, defenderLosses: 1 });
  });

  it("bounds attacker dice by the garrison and defender dice by the defence", () => {
    expect(maxAttackerDice(2)).toBe(1);
    expect(maxAttackerDice(4)).toBe(3);
    expect(maxAttackerDice(9)).toBe(RULES.maxAttackerDice);
    expect(legalDefenderDice(1)).toBe(1);
    expect(legalDefenderDice(5)).toBe(RULES.maxDefenderDice);
  });
});

// ---------------------------------------------------------------------------
// Setup fold
// ---------------------------------------------------------------------------

describe("current aggregate setup", () => {
  it("folds the recorded map snapshot rather than regenerating it", () => {
    const game = startGame();
    const state = game.state();
    expect(state.status).toBe("playing");
    expect(state.map).toBeDefined();
    expect(state.index!.territoryIds).toHaveLength(state.map!.territories.length);
    // The fold reads the snapshot: every dealt territory exists in the map.
    for (const id of Object.keys(state.territories)) {
      expect(state.index!.territoryById.has(id)).toBe(true);
    }
  });

  it("opens round 1 for the first player in reinforce with no pending interrupt", () => {
    const game = startGame();
    const state = game.state();
    expect(state.round).toBe(1);
    expect(state.phase).toBe("reinforce");
    expect(state.activePlayerId).toBe(state.turnOrder[0]);
    expect(state.pendingInteraction).toBeUndefined();
    expect(game.turnId()).toBe(`round-1:${state.turnOrder[0]}`);
  });

  it("records each player's controller", () => {
    const game = startGame({ players: 3, controllers: ["human", "bot", "external-agent"] });
    const state = game.state();
    expect(state.players.map((p) => p.controller)).toEqual(["human", "bot", "external-agent"]);
  });
});

// ---------------------------------------------------------------------------
// Reinforcement
// ---------------------------------------------------------------------------

describe("current reinforcement", () => {
  it("grants base + fully-owned continent bonuses as an explainable breakdown", () => {
    const game = startGame();
    const state = game.state();
    const active = state.activePlayerId!;
    const expectedBase = baseReinforcement(ownedBy(state, active).length);
    expect(state.reinforcement.base).toBe(expectedBase);
    expect(state.reinforcement.total).toBe(
      state.reinforcement.continents.reduce((sum, c) => sum + c.bonus, expectedBase),
    );
    expect(state.reinforcement.remaining).toBe(state.reinforcement.total);
  });

  it("applies a complete allocation atomically and enters attack", () => {
    const game = startGame();
    const total = game.state().reinforcement.total;
    const active = game.state().activePlayerId!;
    const owned = ownedBy(game.state(), active);
    const before = owned.slice(0, 2).map((id) => game.state().territories[id]!.armies);

    const outcome = game.must({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: active,
      placements: [
        { territoryId: owned[0]!, armies: total - 1 },
        { territoryId: owned[1]!, armies: 1 },
      ],
    });

    expect(outcome.status).toBe("accepted");
    if (outcome.status === "accepted") expect(outcome.events).toHaveLength(2);
    expect(game.state().territories[owned[0]!]!.armies).toBe(before[0]! + total - 1);
    expect(game.state().territories[owned[1]!]!.armies).toBe(before[1]! + 1);
    expect(game.state().phase).toBe("attack");
    expect(game.state().reinforcement.remaining).toBe(0);
  });

  it("scores a continent bonus only when every member country is owned", () => {
    const game = startGame();
    const state = game.state();
    const continent = state.map!.continents[0]!;
    expect(continent.reinforcementBonus).toBe(continentBonus(continent.territoryIds.length));

    // Hand the whole continent to one player and re-fold: the bonus appears.
    const owner = state.turnOrder[0]!;
    const patched: AggregateState = {
      ...state,
      territories: Object.fromEntries(
        Object.entries(state.territories).map(([id, t]) => [
          id,
          continent.territoryIds.includes(id) ? { ...t, ownerId: owner } : t,
        ]),
      ),
    };
    expect(controlledContinents(patched, owner).map((c) => c.id)).toContain(continent.id);
    expect(computeReinforcement(patched, owner).continents).toContainEqual({
      continentId: continent.id,
      bonus: continent.reinforcementBonus,
    });

    // Remove one member and the bonus is gone.
    const oneShort: AggregateState = {
      ...patched,
      territories: {
        ...patched.territories,
        [continent.territoryIds[0]!]: {
          ...patched.territories[continent.territoryIds[0]!]!,
          ownerId: "someone-else",
        },
      },
    };
    expect(controlledContinents(oneShort, owner).map((c) => c.id)).not.toContain(continent.id);
  });
});

// ---------------------------------------------------------------------------
// Two-stage combat
// ---------------------------------------------------------------------------

/** Drive the active player into `attack` with a border stack and one declaration. */
function declaredAttack(game: ScriptedGame, faces?: number[]) {
  const setup = armForAttack(game);
  if (faces) game.rig(faces);
  const attackId = declareAttack(game, setup, 3);
  return { setup, attackId };
}

describe("current two-stage combat", () => {
  it("records the attacker's roll at declaration and opens a deadline", () => {
    const game = startGame({ defenseTimeoutMs: 15_000 });
    const declaredAtBefore = game.clock.now;
    const { setup, attackId } = declaredAttack(game, [6, 5, 4]);

    const pending = game.state().pendingInteraction!;
    expect(pending.type).toBe("defense");
    if (pending.type !== "defense") throw new Error("unreachable");
    expect(pending.attackId).toBe(attackId);
    expect(pending.attackerRolls).toEqual([6, 5, 4]);
    expect(pending.attackerId).toBe(setup.attackerId);
    expect(pending.defenderId).toBe(setup.defenderId);
    expect(pending.declaredAt).toBe(declaredAtBefore);
    expect(pending.defenseDeadlineAt).toBe(declaredAtBefore + 15_000);
    expect(pending.defenderDice).toBe(
      legalDefenderDice(game.state().territories[setup.to]!.armies),
    );
    // The turn itself does not move while the interrupt is open.
    expect(game.state().activePlayerId).toBe(setup.attackerId);
    expect(game.state().phase).toBe("attack");
  });

  it("applies losses on resolution and returns to idle attack when no capture", () => {
    const game = startGame();
    const { setup, attackId } = declaredAttack(game, [1, 1, 1]);
    const before = game.state();
    const fromArmies = before.territories[setup.from]!.armies;
    const toArmies = before.territories[setup.to]!.armies;

    game.rig([6, 6]);
    game.must({
      type: "roll-defense",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId,
    });

    const after = game.state();
    const defenderDice = legalDefenderDice(toArmies);
    expect(after.territories[setup.from]!.armies).toBe(fromArmies - defenderDice);
    expect(after.territories[setup.to]!.armies).toBe(toArmies);
    expect(after.pendingInteraction).toBeUndefined();
    expect(after.phase).toBe("attack");
    expect(after.attacks[attackId]!.status).toBe("resolved");
    expect(after.attacks[attackId]!.resolutionSource).toBe("human");
  });

  it("enters a required occupation substate on capture and transfers only on occupy", () => {
    const game = startGame();
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);

    // Ownership has NOT moved yet — only the occupation command transfers it.
    expect(game.state().territories[setup.to]!.ownerId).toBe(setup.defenderId);
    expect(game.state().territories[setup.to]!.armies).toBe(0);
    // A capture wins every compared pair, so the attacker took no losses and can
    // always afford to move the dice it threw.
    expect(pending.maxArmies).toBe(game.state().territories[setup.from]!.armies - 1);
    expect(pending.minArmies).toBeLessThanOrEqual(pending.maxArmies);

    occupyPending(game);
    const after = game.state();
    expect(after.territories[setup.to]!.ownerId).toBe(setup.attackerId);
    expect(after.territories[setup.to]!.armies).toBe(pending.minArmies);
    expect(after.pendingInteraction).toBeUndefined();
    expect(after.attacks[pending.attackId]!.status).toBe("occupied");
  });

  it("does not grow the pool mid-turn when a capture adds territory", () => {
    const game = startGame();
    const poolAtStart = game.state().reinforcement.total;
    const setup = armForAttack(game);
    throwUntilCapture(game, setup);
    occupyPending(game);

    const after = game.state();
    expect(ownedBy(after, setup.attackerId)).toContain(setup.to);
    // The pool is a snapshot taken at the start of the phase: it has not grown,
    // even though the player would now score more base reinforcement.
    expect(after.reinforcement.total).toBe(poolAtStart);
    expect(after.reinforcement.remaining).toBe(0);
    expect(computeReinforcement(after, setup.attackerId).total).toBeGreaterThanOrEqual(poolAtStart);
  });
});

// ---------------------------------------------------------------------------
// Connected fortify and turn order
// ---------------------------------------------------------------------------

describe("current fortify reachability", () => {
  it("reaches through a path of owned countries, not merely neighbours", () => {
    const game = startGame();
    const state = game.state();
    const active = state.activePlayerId!;
    const owned = ownedBy(state, active);
    for (const from of owned) {
      const reachable = friendlyReachable(state, active, from);
      expect(reachable).not.toContain(from);
      for (const to of reachable) expect(state.territories[to]!.ownerId).toBe(active);
      // Reachability is symmetric within a connected owned component.
      for (const to of reachable) expect(friendlyReachable(state, active, to)).toContain(from);
    }
  });

  it("returns nothing for a country the player does not own", () => {
    const state = startGame().state();
    const active = state.activePlayerId!;
    const enemy = Object.values(state.territories).find((t) => t.ownerId !== active)!;
    expect(friendlyReachable(state, active, enemy.id)).toEqual([]);
  });
});

describe("current turn order", () => {
  it("wraps the round and skips eliminated players", () => {
    const game = startGame({ players: 3 });
    const state = game.state();
    const [a, b, c] = state.turnOrder as [string, string, string];
    expect(nextTurn(state, a)).toEqual({ nextPlayerId: b, round: 1 });
    expect(nextTurn(state, c)).toEqual({ nextPlayerId: a, round: 2 });

    const withoutB: AggregateState = {
      ...state,
      players: state.players.map((p) => (p.id === b ? { ...p, eliminated: true } : p)),
    };
    expect(nextTurn(withoutB, a)).toEqual({ nextPlayerId: c, round: 1 });
  });
});

// ---------------------------------------------------------------------------
// Canonical integrity
// ---------------------------------------------------------------------------

describe("current fold integrity", () => {
  it("rejects an AttackResolved whose repeated attacker rolls contradict the declaration", () => {
    const game = startGame();
    const { setup, attackId } = declaredAttack(game, [6, 5, 4]);
    game.must({
      type: "roll-defense",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId,
    });

    const log = game.log.slice();
    const resolvedIndex = log.findIndex((e) => e.type === "AttackResolved");
    const tampered = log.slice();
    tampered[resolvedIndex] = {
      ...(log[resolvedIndex] as Extract<GameEvent, { type: "AttackResolved" }>),
      attackerRolls: [1, 1, 1],
    };
    expect(() => foldAggregate(tampered)).toThrow(AggregateIntegrityError);
  });

  it("rejects an AttackResolved with no pending declaration at all", () => {
    const game = startGame();
    const { setup, attackId } = declaredAttack(game, [6, 5, 4]);
    const declaredIndex = game.log.findIndex((e) => e.type === "AttackDeclared");
    const withoutDeclaration = game.log.filter((_, i) => i !== declaredIndex);
    expect(() =>
      foldAggregate([
        ...withoutDeclaration,
        {
          type: "AttackResolved",
          attackId,
          turnId: game.turnId(),
          attackerId: setup.attackerId,
          defenderId: setup.defenderId,
          from: setup.from,
          to: setup.to,
          attackerRolls: [6, 5, 4],
          defenderRolls: [1],
          attackerLosses: 0,
          defenderLosses: 1,
          territoryCaptured: false,
          resolutionSource: "human",
          commandId: "bogus",
        },
      ]),
    ).toThrow(AggregateIntegrityError);
  });

  it("is a pure function of the event log", () => {
    const game = startGame();
    placeAllReinforcements(game);
    const a = foldAggregate(game.log);
    const b = foldAggregate(game.log);
    expect(JSON.stringify(a.territories)).toBe(JSON.stringify(b.territories));
    expect(a.reinforcement).toEqual(b.reinforcement);
    expect(a.eventCount).toBe(game.log.length);
  });
});
