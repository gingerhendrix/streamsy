/**
 * Structured `Hex Domination` legal-action affordances.
 *
 * Pure and authoritative: derived from a folded {@link AggregateState}, not from
 * the (possibly lagging) board projection.
 *
 * The defining current change is that these are **player-relative**, not
 * active-player-only. While a defence is pending the *defender* — who is not the
 * active player — is the only person with a legal action, and everyone else,
 * including the attacker whose turn it is, has none. During a pending occupation
 * the reverse holds: only the attacker may act.
 */

import type { AggregateState, PendingInteraction } from "../domain/aggregate.ts";
import { friendlyReachable, ownedBy } from "../domain/aggregate.ts";
import { maxAttackerDice } from "../domain/dice.ts";
import { adjacentTo } from "../domain/map.ts";
import { Schema } from "effect";

const MutableArray = <S extends Schema.Top>(schema: S) => Schema.mutable(Schema.Array(schema));
export const LegalAction = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("reinforce"),
    territoryIds: MutableArray(Schema.String),
    pool: Schema.Int,
    submit: Schema.Struct({
      type: Schema.Literal("reinforce"),
      placements: MutableArray(
        Schema.Struct({
          territoryId: Schema.Literal("<one of territoryIds>"),
          armies: Schema.Literal("<1..pool>"),
        }),
      ),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("declare-attack"),
    choices: MutableArray(
      Schema.Struct({ from: Schema.String, to: Schema.String, maxAttackerDice: Schema.Int }),
    ),
    submit: Schema.Struct({
      type: Schema.Literal("declare-attack"),
      from: Schema.Literal("<choice.from>"),
      to: Schema.Literal("<choice.to>"),
      attackerDice: Schema.Literal("<1..choice.maxAttackerDice>"),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("roll-defense"),
    attackId: Schema.String,
    dice: Schema.Int,
    deadlineAt: Schema.Finite,
    submit: Schema.Struct({
      type: Schema.Literal("roll-defense"),
      attackId: Schema.Literal("<attackId>"),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("occupy-territory"),
    attackId: Schema.String,
    from: Schema.String,
    to: Schema.String,
    minArmies: Schema.Int,
    maxArmies: Schema.Int,
    submit: Schema.Struct({
      type: Schema.Literal("occupy-territory"),
      attackId: Schema.Literal("<attackId>"),
      armies: Schema.Literal("<minArmies..maxArmies>"),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("fortify"),
    choices: MutableArray(
      Schema.Struct({
        from: Schema.String,
        reachable: MutableArray(Schema.Struct({ to: Schema.String, maxArmies: Schema.Int })),
      }),
    ),
    submit: Schema.Struct({
      type: Schema.Literal("fortify"),
      from: Schema.Literal("<choice.from>"),
      to: Schema.Literal("<choice.reachable.to>"),
      armies: Schema.Literal("<1..choice.reachable.maxArmies>"),
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("skip-fortifications"),
    submit: Schema.Struct({ type: Schema.Literal("skip-fortifications") }),
  }),
]);
export type LegalAction = typeof LegalAction.Type;

/** How the decision resource labels this player's relationship to the moment. */
export const DecisionMode = Schema.Literals(["active-turn", "defense", "waiting", "finished"]);
export type DecisionMode = typeof DecisionMode.Type;

export function decisionMode(state: AggregateState, playerId: string): DecisionMode {
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
  state: AggregateState,
  pending: PendingInteraction,
  playerId: string,
): LegalAction[] | undefined {
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
export function legalActions(state: AggregateState, playerId: string): LegalAction[] {
  if (state.status !== "playing" || !state.index) return [];

  // A pending interrupt suspends every ordinary affordance, for everyone.
  if (state.pendingInteraction) {
    return pendingActions(state, state.pendingInteraction, playerId) ?? [];
  }
  if (state.activePlayerId !== playerId) return [];

  const owned = ownedBy(state, playerId);

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
      for (const to of adjacentTo(state.index, from)) {
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
    const actions: LegalAction[] = [];
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
    actions.push({
      type: "skip-fortifications",
      submit: { type: "skip-fortifications" },
    });
    return actions;
  }

  // A fortify command ends the turn atomically, so this state is only observable
  // while replaying a persisted history with an intermediate fortify phase.
  return [];
}
