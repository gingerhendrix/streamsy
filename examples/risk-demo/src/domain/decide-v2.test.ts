import { describe, expect, it } from "vitest";

import { ownedByV2 } from "./aggregate-v2.ts";
import type { PendingInteraction } from "./aggregate-v2.ts";
import { legalActionsV2, decisionModeV2 } from "../application/legal-actions-v2.ts";
import { RULES_V2 } from "./map-v2.ts";
import {
  armForAttack,
  declareAttackV2,
  nextCommandIdV2,
  placeAllReinforcementsV2,
  startGameV2,
  throwUntilCapture,
  winThrow,
  type ScriptedGameV2,
} from "../../test/testkit-v2.ts";

function expectRejected(outcome: ReturnType<ScriptedGameV2["submit"]>, code: string): void {
  expect(outcome.status).toBe("rejected");
  if (outcome.status !== "rejected") return;
  expect(outcome.error.code).toBe(code);
}

/** Arm the active player and open a defence interrupt. */
function pendingDefense(game: ScriptedGameV2, faces = [3, 3, 3]) {
  const setup = armForAttack(game);
  game.rig(faces);
  const attackId = declareAttackV2(game, setup, 3);
  const pending = game.state().pendingInteraction as Extract<
    PendingInteraction,
    { type: "defense" }
  >;
  return { setup, attackId, pending };
}

// ---------------------------------------------------------------------------
// Turn discipline
// ---------------------------------------------------------------------------

describe("v2 turn discipline", () => {
  it("rejects a command from the wrong player and a stale turnId", () => {
    const game = startGameV2();
    const state = game.state();
    const active = state.activePlayerId!;
    const other = state.turnOrder.find((id) => id !== active)!;

    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: other,
        placements: [{ territoryId: ownedByV2(state, other)[0]!, armies: 1 }],
      }),
      "NOT_YOUR_TURN",
    );

    const stale = game.submit({
      type: "reinforce",
      commandId: nextCommandIdV2(),
      turnId: "round-99:nobody",
      playerId: active,
      placements: [{ territoryId: ownedByV2(state, active)[0]!, armies: 1 }],
    });
    expectRejected(stale, "STALE_TURN");
    if (stale.status === "rejected") expect(stale.error.currentTurnId).toBe(game.turnId());
  });

  it("returns the original events for a duplicate commandId without re-rolling", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    game.rig([6, 5, 4]);
    const commandId = nextCommandIdV2("dup");
    const command = {
      type: "declare-attack" as const,
      commandId,
      turnId: game.turnId(),
      playerId: setup.attackerId,
      from: setup.from,
      to: setup.to,
      attackerDice: 3,
    };
    const first = game.must(command);
    const retry = game.submit(command);
    expect(retry.status).toBe("duplicate");
    if (retry.status === "rejected") throw new Error("unreachable");
    expect(retry.events).toEqual(first.status === "rejected" ? [] : first.events);
  });

  it("requires one exact, unique allocation across owned territories", () => {
    const game = startGameV2();
    const state = game.state();
    const active = state.activePlayerId!;
    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        placements: [
          {
            territoryId: ownedByV2(state, active)[0]!,
            armies: state.reinforcement.remaining + 1,
          },
        ],
      }),
      "INSUFFICIENT_ARMIES",
    );
    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        placements: [{ territoryId: ownedByV2(state, active)[0]!, armies: 1 }],
      }),
      "INSUFFICIENT_ARMIES",
    );
    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        placements: [
          { territoryId: ownedByV2(state, active)[0]!, armies: 1 },
          {
            territoryId: ownedByV2(state, active)[0]!,
            armies: state.reinforcement.remaining - 1,
          },
        ],
      }),
      "ILLEGAL_ACTION",
    );
    const enemy = Object.values(state.territories).find((t) => t.ownerId !== active)!;
    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        placements: [{ territoryId: enemy.id, armies: 1 }],
      }),
      "ILLEGAL_ACTION",
    );
    expectRejected(
      game.submit({
        type: "reinforce",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        placements: [{ territoryId: "no-such-country", armies: 1 }],
      }),
      "UNKNOWN_TERRITORY",
    );
  });

  it("blocks attacking and ending the turn until the pool is placed", () => {
    const game = startGameV2();
    const active = game.state().activePlayerId!;
    expectRejected(
      game.submit({
        type: "skip-fortifications",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
      }),
      "INVALID_PHASE",
    );
  });
});

// ---------------------------------------------------------------------------
// Declaration bounds
// ---------------------------------------------------------------------------

describe("v2 declare-attack", () => {
  it("bounds attacker dice by the source garrison", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    const armies = game.state().territories[setup.from]!.armies;
    expectRejected(
      game.submit({
        type: "declare-attack",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.attackerId,
        from: setup.from,
        to: setup.to,
        attackerDice: RULES_V2.maxAttackerDice + 1,
      }),
      "ILLEGAL_ACTION",
    );
    // A one-army country cannot attack at all: dice must leave one behind.
    const lone = Object.values(game.state().territories).find(
      (t) => t.ownerId === setup.attackerId && t.armies === 1,
    );
    if (lone) {
      const target = game
        .state()
        .index!.territoryById.get(lone.id)!
        .adjacentTerritoryIds.find(
          (id) => game.state().territories[id]!.ownerId !== setup.attackerId,
        );
      if (target) {
        expectRejected(
          game.submit({
            type: "declare-attack",
            commandId: nextCommandIdV2(),
            turnId: game.turnId(),
            playerId: setup.attackerId,
            from: lone.id,
            to: target,
            attackerDice: 1,
          }),
          "INSUFFICIENT_ARMIES",
        );
      }
    }
    expect(armies).toBeGreaterThan(1);
  });

  it("refuses a non-adjacent target and an own-country target", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    const state = game.state();
    const nonAdjacent = Object.values(state.territories).find(
      (t) =>
        t.ownerId !== setup.attackerId &&
        !state.index!.territoryById.get(setup.from)!.adjacentTerritoryIds.includes(t.id),
    );
    if (nonAdjacent) {
      const rejected = game.submit({
        type: "declare-attack",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.attackerId,
        from: setup.from,
        to: nonAdjacent.id,
        attackerDice: 1,
      });
      expectRejected(rejected, "NOT_ADJACENT");
      if (rejected.status === "rejected") {
        expect(rejected.error.message).toContain(
          state.index!.territoryById.get(nonAdjacent.id)!.name,
        );
        expect(rejected.error.message).toContain(state.index!.territoryById.get(setup.from)!.name);
        expect(rejected.error.message).toContain("not neighbours");
      }
    }
    const ownNeighbour = state
      .index!.territoryById.get(setup.from)!
      .adjacentTerritoryIds.find((id) => state.territories[id]!.ownerId === setup.attackerId);
    if (ownNeighbour) {
      expectRejected(
        game.submit({
          type: "declare-attack",
          commandId: nextCommandIdV2(),
          turnId: game.turnId(),
          playerId: setup.attackerId,
          from: setup.from,
          to: ownNeighbour,
          attackerDice: 1,
        }),
        "ILLEGAL_ACTION",
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Pending defence excludes every other command
// ---------------------------------------------------------------------------

describe("v2 pending defence", () => {
  it("suspends every command except the defender's roll", () => {
    const game = startGameV2();
    const { setup, attackId } = pendingDefense(game);
    const turnId = game.turnId();
    const owned = ownedByV2(game.state(), setup.attackerId);

    for (const command of [
      {
        type: "reinforce" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        placements: [{ territoryId: owned[0]!, armies: 1 }],
      },
      {
        type: "declare-attack" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        from: setup.from,
        to: setup.to,
        attackerDice: 1,
      },
      {
        type: "fortify" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        from: owned[0]!,
        to: owned[1]!,
        armies: 1,
      },
      {
        type: "skip-fortifications" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
      },
    ]) {
      expectRejected(game.submit(command), "PENDING_DEFENSE");
    }

    // Occupation is not legal either — nothing has been captured yet.
    expectRejected(
      game.submit({
        type: "occupy-territory",
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        attackId,
        armies: 1,
      }),
      "PENDING_DEFENSE",
    );
  });

  it("only the named defender may roll, for the named attack, on the named turn", () => {
    const game = startGameV2({ players: 3 });
    const { setup, attackId } = pendingDefense(game);
    const bystander = game
      .state()
      .turnOrder.find((id) => id !== setup.defenderId && id !== setup.attackerId)!;

    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: bystander,
        attackId,
      }),
      "NOT_DEFENDING_PLAYER",
    );
    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.attackerId,
        attackId,
      }),
      "NOT_DEFENDING_PLAYER",
    );
    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.defenderId,
        attackId: "some-other-attack",
      }),
      "ATTACK_ID_MISMATCH",
    );
    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: "round-9:someone",
        playerId: setup.defenderId,
        attackId,
      }),
      "STALE_TURN",
    );
  });

  it("rejects a human roll that arrives after the canonical deadline", () => {
    const game = startGameV2({ defenseTimeoutMs: 1_000 });
    const { setup, attackId, pending } = pendingDefense(game);
    game.advanceClock(1_001);
    expect(game.clock.now).toBeGreaterThan(pending.defenseDeadlineAt);

    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.defenderId,
        attackId,
      }),
      "DEFENSE_DEADLINE_EXPIRED",
    );
    // The interrupt is still open: the timeout resolver owns it from here.
    expect(game.state().pendingInteraction?.type).toBe("defense");
  });

  it("lets the timeout resolver close a combat the deadline has passed on", () => {
    const game = startGameV2({ defenseTimeoutMs: 1_000 });
    const { attackId } = pendingDefense(game);
    game.advanceClock(5_000);
    const outcome = game.must({
      type: "resolve-defense-timeout",
      commandId: `timeout-defense:${attackId}`,
      turnId: game.turnId(),
      attackId,
    });
    if (outcome.status === "rejected") throw new Error("unreachable");
    const resolved = outcome.events[0]!;
    expect(resolved.type).toBe("AttackResolved");
    if (resolved.type !== "AttackResolved") return;
    expect(resolved.resolutionSource).toBe("timeout");
  });

  it("makes a second resolver lose after the first has committed", () => {
    const game = startGameV2();
    const { setup, attackId } = pendingDefense(game);
    game.must({
      type: "roll-defense",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId,
    });
    // A timeout job delivered late refolds and finds nothing pending.
    expectRejected(
      game.submit({
        type: "resolve-defense-timeout",
        commandId: `timeout-defense:${attackId}`,
        turnId: game.turnId(),
        attackId,
      }),
      "ATTACK_ALREADY_RESOLVED",
    );
    // So does a duplicate human roll under a fresh commandId.
    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: setup.defenderId,
        attackId,
      }),
      "ATTACK_ALREADY_RESOLVED",
    );
  });

  it("cannot be resolved by a stale timer naming an earlier attack", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    game.rig([3, 3, 3]);
    const firstAttack = declareAttackV2(game, setup, 3);
    game.must({
      type: "roll-defense",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId: firstAttack,
    });
    if (game.state().pendingInteraction?.type === "occupation") return; // captured; not this case

    const secondAttack = declareAttackV2(game, setup, 1);
    expect(game.state().pendingInteraction?.type).toBe("defense");

    // The first attack's timer fires late: it must not resolve the second attack.
    expectRejected(
      game.submit({
        type: "resolve-defense-timeout",
        commandId: `timeout-defense:${firstAttack}`,
        turnId: game.turnId(),
        attackId: firstAttack,
      }),
      "ATTACK_ALREADY_RESOLVED",
    );
    expect(
      (game.state().pendingInteraction as Extract<PendingInteraction, { type: "defense" }>)
        .attackId,
    ).toBe(secondAttack);
  });
});

// ---------------------------------------------------------------------------
// Pending occupation
// ---------------------------------------------------------------------------

describe("v2 pending occupation", () => {
  it("suspends every command except the attacker's occupation", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);
    const turnId = game.turnId();
    const owned = ownedByV2(game.state(), setup.attackerId);

    for (const command of [
      {
        type: "reinforce" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        placements: [{ territoryId: owned[0]!, armies: 1 }],
      },
      {
        type: "declare-attack" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        from: setup.from,
        to: setup.to,
        attackerDice: 1,
      },
      {
        type: "fortify" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        from: owned[0]!,
        to: owned[1]!,
        armies: 1,
      },
      {
        type: "skip-fortifications" as const,
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
      },
    ]) {
      expectRejected(game.submit(command), "PENDING_OCCUPATION");
    }
    // The defender cannot roll again either — that combat is closed.
    expectRejected(
      game.submit({
        type: "roll-defense",
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.defenderId,
        attackId: pending.attackId,
      }),
      "ATTACK_ALREADY_RESOLVED",
    );
  });

  it("enforces the occupation bounds exactly", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);
    const turnId = game.turnId();

    for (const armies of [pending.minArmies - 1, pending.maxArmies + 1, 0, 1.5]) {
      expectRejected(
        game.submit({
          type: "occupy-territory",
          commandId: nextCommandIdV2(),
          turnId,
          playerId: setup.attackerId,
          attackId: pending.attackId,
          armies,
        }),
        "INVALID_OCCUPATION",
      );
    }
    expectRejected(
      game.submit({
        type: "occupy-territory",
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.defenderId,
        attackId: pending.attackId,
        armies: pending.minArmies,
      }),
      "NOT_YOUR_TURN",
    );
    expectRejected(
      game.submit({
        type: "occupy-territory",
        commandId: nextCommandIdV2(),
        turnId,
        playerId: setup.attackerId,
        attackId: "not-this-attack",
        armies: pending.minArmies,
      }),
      "ATTACK_ID_MISMATCH",
    );

    // The maximum always leaves exactly one army behind.
    game.must({
      type: "occupy-territory",
      commandId: nextCommandIdV2(),
      turnId,
      playerId: setup.attackerId,
      attackId: pending.attackId,
      armies: pending.maxArmies,
    });
    expect(game.state().territories[setup.from]!.armies).toBe(1);
    expect(game.state().territories[setup.to]!.armies).toBe(pending.maxArmies);
  });
});

// ---------------------------------------------------------------------------
// Fortify
// ---------------------------------------------------------------------------

describe("v2 fortify", () => {
  it("moves armies and ends the turn in one canonical command", () => {
    const game = startGameV2();
    placeAllReinforcementsV2(game);
    const state = game.state();
    const active = state.activePlayerId!;
    const fortify = legalActionsV2(state, active).find((a) => a.type === "fortify");
    expect(fortify).toBeDefined();
    expect(legalActionsV2(state, active)).toContainEqual({
      type: "skip-fortifications",
      submit: { type: "skip-fortifications" },
    });
    expect(legalActionsV2(state, active).map((action) => action.type)).not.toContain("end-turn");
    if (fortify?.type !== "fortify") return;
    const choice = fortify.choices.find((c) => c.reachable.length > 0)!;
    const destination = choice.reachable[0]!;

    const beforeFrom = state.territories[choice.from]!.armies;
    const beforeTo = state.territories[destination.to]!.armies;
    const outcome = game.must({
      type: "fortify",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: active,
      from: choice.from,
      to: destination.to,
      armies: 1,
    });
    expect(outcome.status).toBe("accepted");
    if (outcome.status !== "accepted") return;
    expect(outcome.events.map((event) => event.type)).toEqual(["ArmiesFortified", "TurnEnded"]);
    const after = game.state();
    expect(after.activePlayerId).not.toBe(active);
    expect(after.phase).toBe("reinforce");
    expect(after.territories[choice.from]!.armies).toBe(beforeFrom - 1);
    expect(after.territories[destination.to]!.armies).toBe(beforeTo + 1);
    expect(legalActionsV2(after, active)).toEqual([]);
  });

  it("skips the optional fortification and ends the turn", () => {
    const game = startGameV2();
    placeAllReinforcementsV2(game);
    const active = game.state().activePlayerId!;
    const outcome = game.must({
      type: "skip-fortifications",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: active,
    });

    expect(outcome.status).toBe("accepted");
    if (outcome.status !== "accepted") return;
    expect(outcome.events.map((event) => event.type)).toEqual(["TurnEnded"]);
    expect(game.state().activePlayerId).not.toBe(active);
    expect(game.state().phase).toBe("reinforce");
  });

  it("rejects a destination with no friendly path", () => {
    const game = startGameV2();
    placeAllReinforcementsV2(game);
    const state = game.state();
    const active = state.activePlayerId!;
    const enemy = Object.values(state.territories).find((t) => t.ownerId !== active)!;
    const source = ownedByV2(state, active).find((id) => state.territories[id]!.armies > 1)!;

    // An enemy country is not a legal destination at all.
    expectRejected(
      game.submit({
        type: "fortify",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: active,
        from: source,
        to: enemy.id,
        armies: 1,
      }),
      "ILLEGAL_ACTION",
    );

    // An owned country in a *different* connected component is NO_FRIENDLY_PATH.
    const reachable = new Set(
      (legalActionsV2(state, active).find((a) => a.type === "fortify") as any)?.choices
        .find((c: any) => c.from === source)
        ?.reachable.map((r: any) => r.to) ?? [],
    );
    const disconnected = ownedByV2(state, active).find((id) => id !== source && !reachable.has(id));
    if (disconnected) {
      expectRejected(
        game.submit({
          type: "fortify",
          commandId: nextCommandIdV2(),
          turnId: game.turnId(),
          playerId: active,
          from: source,
          to: disconnected,
          armies: 1,
        }),
        "NO_FRIENDLY_PATH",
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Legal actions are player-relative
// ---------------------------------------------------------------------------

describe("v2 legal actions", () => {
  it("gives the out-of-turn defender the only action while defence is pending", () => {
    const game = startGameV2({ players: 3, defenseTimeoutMs: 15_000 });
    const { setup, attackId, pending } = pendingDefense(game);
    const state = game.state();
    const bystander = state.turnOrder.find(
      (id) => id !== setup.defenderId && id !== setup.attackerId,
    )!;

    expect(legalActionsV2(state, setup.defenderId)).toEqual([
      {
        type: "roll-defense",
        attackId,
        dice: pending.defenderDice,
        deadlineAt: pending.defenseDeadlineAt,
        submit: { type: "roll-defense", attackId: "<attackId>" },
      },
    ]);
    expect(legalActionsV2(state, setup.attackerId)).toEqual([]);
    expect(legalActionsV2(state, bystander)).toEqual([]);

    expect(decisionModeV2(state, setup.defenderId)).toBe("defense");
    expect(decisionModeV2(state, setup.attackerId)).toBe("waiting");
    expect(decisionModeV2(state, bystander)).toBe("waiting");
  });

  it("gives only the attacker an action while occupation is pending", () => {
    const game = startGameV2();
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);
    const state = game.state();

    expect(legalActionsV2(state, setup.attackerId)).toEqual([
      {
        type: "occupy-territory",
        attackId: pending.attackId,
        from: pending.from,
        to: pending.to,
        minArmies: pending.minArmies,
        maxArmies: pending.maxArmies,
        submit: {
          type: "occupy-territory",
          attackId: "<attackId>",
          armies: "<minArmies..maxArmies>",
        },
      },
    ]);
    expect(legalActionsV2(state, setup.defenderId)).toEqual([]);
    expect(decisionModeV2(state, setup.attackerId)).toBe("active-turn");
  });
});

// ---------------------------------------------------------------------------
// Elimination and victory
// ---------------------------------------------------------------------------

describe("v2 elimination and victory", () => {
  /** All countries to `attacker` except one lone enemy holding, adjacent to a stack. */
  function nearFinalBoard(players: number) {
    return startGameV2({
      players,
      board: ({ map, turnOrder, initialTerritories }) => {
        const attacker = turnOrder[0]!;
        const victim = turnOrder[1]!;
        // The victim's last country neighbours the attacker's staging country.
        const lastEnemy = map.territories.find((t) => t.adjacentTerritoryIds.length > 0)!;
        const source = lastEnemy.adjacentTerritoryIds[0]!;
        return {
          initialTerritories: initialTerritories.map((t) => {
            if (t.territoryId === lastEnemy.id) {
              return { ...t, ownerId: victim, armies: 1 };
            }
            if (t.territoryId === source) return { ...t, ownerId: attacker, armies: 6 };
            // A third player, if present, keeps one far-away country so the game
            // is not already won when the victim falls.
            const spare = turnOrder[2];
            if (spare && t.territoryId === map.territories.at(-1)!.id) {
              return { ...t, ownerId: spare, armies: 1 };
            }
            return { ...t, ownerId: attacker, armies: 1 };
          }),
        };
      },
    });
  }

  /** Reinforce, then capture the victim's last country. */
  function conquerLastCountry(game: ScriptedGameV2) {
    const state = game.state();
    const attacker = state.activePlayerId!;
    const victim = state.turnOrder.find(
      (id) => id !== attacker && ownedByV2(state, id).length === 1,
    )!;
    const to = ownedByV2(state, victim)[0]!;
    const from = state
      .index!.territoryById.get(to)!
      .adjacentTerritoryIds.find((id) => state.territories[id]!.ownerId === attacker)!;

    placeAllReinforcementsV2(game, from);
    winThrow(game, { from, to, attackerId: attacker, defenderId: victim }, 1);
    const pending = game.state().pendingInteraction as Extract<
      PendingInteraction,
      { type: "occupation" }
    >;
    expect(pending.type).toBe("occupation");
    const outcome = game.must({
      type: "occupy-territory",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: attacker,
      attackId: pending.attackId,
      armies: pending.minArmies,
    });
    if (outcome.status === "rejected") throw new Error("unreachable");
    return { attacker, victim, events: outcome.events };
  }

  it("eliminates the loser in the same atomic batch as the occupation", () => {
    const game = nearFinalBoard(3);
    const { attacker, victim, events } = conquerLastCountry(game);

    expect(events.map((e) => e.type)).toEqual(["TerritoryOccupied", "PlayerEliminated"]);
    const eliminated = events[1]!;
    if (eliminated.type !== "PlayerEliminated") throw new Error("unreachable");
    expect(eliminated.playerId).toBe(victim);
    expect(eliminated.byPlayerId).toBe(attacker);

    const state = game.state();
    expect(state.players.find((p) => p.id === victim)!.eliminated).toBe(true);
    expect(state.status).toBe("playing");
    // The eliminated player is skipped in turn order and can no longer act.
    expectRejected(
      game.submit({
        type: "skip-fortifications",
        commandId: nextCommandIdV2(),
        turnId: game.turnId(),
        playerId: victim,
      }),
      "NOT_YOUR_TURN",
    );
    game.must({
      type: "skip-fortifications",
      commandId: nextCommandIdV2(),
      turnId: game.turnId(),
      playerId: attacker,
    });
    expect(game.state().activePlayerId).not.toBe(victim);
  });

  it("wins the game when the last opponent's last country falls", () => {
    const game = nearFinalBoard(2);
    const { attacker, events } = conquerLastCountry(game);

    expect(events.map((e) => e.type)).toEqual(["TerritoryOccupied", "PlayerEliminated", "GameWon"]);
    const state = game.state();
    expect(state.status).toBe("finished");
    expect(state.winnerId).toBe(attacker);
    expect(state.activePlayerId).toBeUndefined();
    expect(state.pendingInteraction).toBeUndefined();
    expect(Object.values(state.territories).every((t) => t.ownerId === attacker)).toBe(true);

    expectRejected(
      game.submit({
        type: "skip-fortifications",
        commandId: nextCommandIdV2(),
        turnId: `round-1:${attacker}`,
        playerId: attacker,
      }),
      "GAME_FINISHED",
    );
  });
});
