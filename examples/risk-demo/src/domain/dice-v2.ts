/**
 * V2 dice comparison.
 *
 * Unlike v1's `./dice.ts`, nothing here consumes randomness. The two halves of a
 * v2 throw are rolled at different times by different actors — the attacker's at
 * declaration, the defender's at resolution — so the comparison is a pure
 * function of two recorded roll arrays. That is what lets the aggregate and the
 * board projection both re-derive losses from `AttackResolved` without ever
 * touching an `Rng`.
 *
 * Rolls are sorted descending and compared pairwise; the higher die wins and ties
 * favour the defender. Only `min(attacker, defender)` pairs are compared, so
 * extra attacker dice beyond the defender's count are simply unused.
 */

import { RULES_V2 } from "./map-v2.ts";
import type { Rng } from "./rng.ts";
import { rollDice } from "./rng.ts";

export const descending = (a: number, b: number): number => b - a;

export interface CombatComparison {
  attackerLosses: number;
  defenderLosses: number;
}

export function compareRolls(
  attackerRolls: readonly number[],
  defenderRolls: readonly number[],
): CombatComparison {
  const attacker = attackerRolls.toSorted(descending);
  const defender = defenderRolls.toSorted(descending);
  let attackerLosses = 0;
  let defenderLosses = 0;
  const pairs = Math.min(attacker.length, defender.length);
  for (let i = 0; i < pairs; i += 1) {
    if (attacker[i]! > defender[i]!) defenderLosses += 1;
    else attackerLosses += 1;
  }
  return { attackerLosses, defenderLosses };
}

/** Attacker dice are bounded by the source garrison: one army must stay behind. */
export function maxAttackerDice(sourceArmies: number): number {
  return Math.min(RULES_V2.maxAttackerDice, sourceArmies - 1);
}

/** The defender's dice count is not a choice — it is the maximum legal count. */
export function legalDefenderDice(defendingArmies: number): number {
  return Math.min(RULES_V2.maxDefenderDice, defendingArmies);
}

/** Roll `count` dice through the injected Rng, sorted descending for recording. */
export function rollSorted(rng: Rng, count: number): number[] {
  return rollDice(rng, count).toSorted(descending);
}
