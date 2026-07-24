/**
 * Structured legal-action affordances for the active player.
 *
 * Pure and authoritative: derived from a folded {@link AggregateState}, not from
 * the (possibly lagging) board projection. Mirrors the `LegalAction` shape in
 * `agent-play-api.md` so coding agents can choose a move without scraping a UI.
 */

import type { AggregateState } from "../domain/aggregate.ts";
import { RULES, TERRITORIES, adjacentTo } from "../domain/map.ts";

export type LegalAction =
  | { type: "reinforce"; territoryIds: string[]; minArmies: number; maxArmies: number }
  | { type: "attack"; choices: Array<{ from: string; to: string; maxAttackerDice: number }> }
  | { type: "fortify"; choices: Array<{ from: string; to: string; maxArmies: number }> }
  | { type: "end-turn" };

function ownedTerritoryIds(state: AggregateState, playerId: string): string[] {
  return TERRITORIES.map((t) => t.id).filter((id) => state.territories[id]?.ownerId === playerId);
}

/**
 * The actions the given player may legally submit right now. Empty when the game
 * is not in progress or it is not this player's turn.
 */
export function legalActions(state: AggregateState, playerId: string): LegalAction[] {
  if (state.status !== "playing" || state.activePlayerId !== playerId) return [];

  if (state.phase === "reinforce") {
    const territoryIds = ownedTerritoryIds(state, playerId);
    if (state.reinforcementsRemaining <= 0 || territoryIds.length === 0) return [];
    return [
      { type: "reinforce", territoryIds, minArmies: 1, maxArmies: state.reinforcementsRemaining },
    ];
  }

  if (state.phase === "attack") {
    const owned = ownedTerritoryIds(state, playerId);
    const attackChoices: Array<{ from: string; to: string; maxAttackerDice: number }> = [];
    const fortifyChoices: Array<{ from: string; to: string; maxArmies: number }> = [];
    for (const from of owned) {
      const armies = state.territories[from]!.armies;
      for (const to of adjacentTo(from)) {
        const target = state.territories[to]!;
        if (target.ownerId !== playerId && armies >= 2) {
          attackChoices.push({
            from,
            to,
            maxAttackerDice: Math.min(RULES.maxAttackerDice, armies - 1),
          });
        }
        if (target.ownerId === playerId && armies >= 2) {
          fortifyChoices.push({ from, to, maxArmies: armies - 1 });
        }
      }
    }
    const actions: LegalAction[] = [];
    if (attackChoices.length > 0) actions.push({ type: "attack", choices: attackChoices });
    if (fortifyChoices.length > 0) actions.push({ type: "fortify", choices: fortifyChoices });
    actions.push({ type: "end-turn" });
    return actions;
  }

  // fortify phase: the single maneuver is spent; only ending the turn remains.
  return [{ type: "end-turn" }];
}
