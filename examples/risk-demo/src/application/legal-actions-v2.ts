/**
 * Structured `risk-demo-v2` legal-action affordances (design spec §6.2).
 *
 * Pure and authoritative: derived from a folded {@link AggregateStateV2}, not from
 * the (possibly lagging) board projection.
 *
 * The defining v2 change is that these are **player-relative**, not
 * active-player-only. While a defence is pending the *defender* — who is not the
 * active player — is the only person with a legal action, and everyone else,
 * including the attacker whose turn it is, has none. During a pending occupation
 * the reverse holds: only the attacker may act.
 */

import type { AggregateStateV2, PendingInteraction } from "../domain/aggregate-v2.ts";
import { friendlyReachable, ownedByV2 } from "../domain/aggregate-v2.ts";
import { maxAttackerDice } from "../domain/dice-v2.ts";
import { adjacentToV2 } from "../domain/map-v2.ts";

export type LegalActionV2 =
  | {
      type: "reinforce";
      territoryIds: string[];
      pool: number;
      submit: {
        type: "reinforce";
        placements: Array<{ territoryId: "<one of territoryIds>"; armies: "<1..pool>" }>;
      };
    }
  | {
      type: "declare-attack";
      choices: Array<{ from: string; to: string; maxAttackerDice: number }>;
      submit: {
        type: "declare-attack";
        from: "<choice.from>";
        to: "<choice.to>";
        attackerDice: "<1..choice.maxAttackerDice>";
      };
    }
  | {
      type: "roll-defense";
      attackId: string;
      dice: number;
      deadlineAt: number;
      submit: { type: "roll-defense"; attackId: "<attackId>" };
    }
  | {
      type: "occupy-territory";
      attackId: string;
      from: string;
      to: string;
      minArmies: number;
      maxArmies: number;
      submit: {
        type: "occupy-territory";
        attackId: "<attackId>";
        armies: "<minArmies..maxArmies>";
      };
    }
  | {
      type: "fortify";
      choices: Array<{ from: string; reachable: Array<{ to: string; maxArmies: number }> }>;
      submit: {
        type: "fortify";
        from: "<choice.from>";
        to: "<choice.reachable.to>";
        armies: "<1..choice.reachable.maxArmies>";
      };
    }
  | { type: "end-turn"; submit: { type: "end-turn" } };

/** How the decision resource labels this player's relationship to the moment. */
export type DecisionModeV2 = "active-turn" | "defense" | "waiting" | "finished";

export function decisionModeV2(state: AggregateStateV2, playerId: string): DecisionModeV2 {
  if (state.status === "finished") return "finished";
  const pending = state.pendingInteraction;
  if (pending?.type === "defense") {
    const defender = state.players.find((player) => player.id === playerId);
    return pending.defenderId === playerId && defender?.controller !== "external-agent"
      ? "defense"
      : "waiting";
  }
  if (pending?.type === "occupation") {
    return pending.playerId === playerId ? "active-turn" : "waiting";
  }
  return state.activePlayerId === playerId ? "active-turn" : "waiting";
}

function pendingActions(
  state: AggregateStateV2,
  pending: PendingInteraction,
  playerId: string,
): LegalActionV2[] | undefined {
  if (pending.type === "defense") {
    const defender = state.players.find((player) => player.id === playerId);
    if (pending.defenderId !== playerId || defender?.controller === "external-agent") return [];
    return [
      {
        type: "roll-defense",
        attackId: pending.attackId,
        dice: pending.defenderDice,
        deadlineAt: pending.defenseDeadlineAt,
        submit: { type: "roll-defense", attackId: "<attackId>" },
      },
    ];
  }
  if (pending.playerId !== playerId) return [];
  return [
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
  ];
}

/**
 * The actions the given player may legally submit right now. Empty when the game
 * is not in progress, or when someone else holds the only open decision.
 */
export function legalActionsV2(state: AggregateStateV2, playerId: string): LegalActionV2[] {
  if (state.status !== "playing" || !state.index) return [];

  // A pending interrupt suspends every ordinary affordance, for everyone.
  if (state.pendingInteraction) {
    return pendingActions(state, state.pendingInteraction, playerId) ?? [];
  }
  if (state.activePlayerId !== playerId) return [];

  const owned = ownedByV2(state, playerId);

  if (state.phase === "reinforce") {
    const remaining = state.reinforcement.remaining;
    if (remaining <= 0 || owned.length === 0) return [];
    return [
      {
        type: "reinforce",
        territoryIds: owned,
        pool: remaining,
        submit: {
          type: "reinforce",
          placements: [{ territoryId: "<one of territoryIds>", armies: "<1..pool>" }],
        },
      },
    ];
  }

  if (state.phase === "attack") {
    const attackChoices: Array<{ from: string; to: string; maxAttackerDice: number }> = [];
    const fortifyChoices: Array<{
      from: string;
      reachable: Array<{ to: string; maxArmies: number }>;
    }> = [];
    for (const from of owned) {
      const armies = state.territories[from]!.armies;
      if (armies < 2) continue;
      for (const to of adjacentToV2(state.index, from)) {
        if (state.territories[to]?.ownerId !== playerId) {
          attackChoices.push({ from, to, maxAttackerDice: maxAttackerDice(armies) });
        }
      }
      // Fortify reaches through any path of owned countries, not just neighbours.
      const reachable = friendlyReachable(state, playerId, from).map((to) => ({
        to,
        maxArmies: armies - 1,
      }));
      if (reachable.length > 0) fortifyChoices.push({ from, reachable });
    }
    const actions: LegalActionV2[] = [];
    if (attackChoices.length > 0)
      actions.push({
        type: "declare-attack",
        choices: attackChoices,
        submit: {
          type: "declare-attack",
          from: "<choice.from>",
          to: "<choice.to>",
          attackerDice: "<1..choice.maxAttackerDice>",
        },
      });
    if (fortifyChoices.length > 0)
      actions.push({
        type: "fortify",
        choices: fortifyChoices,
        submit: {
          type: "fortify",
          from: "<choice.from>",
          to: "<choice.reachable.to>",
          armies: "<1..choice.reachable.maxArmies>",
        },
      });
    actions.push({ type: "end-turn", submit: { type: "end-turn" } });
    return actions;
  }

  // fortify phase: the single manoeuvre is spent; only ending the turn remains.
  return [{ type: "end-turn", submit: { type: "end-turn" } }];
}
