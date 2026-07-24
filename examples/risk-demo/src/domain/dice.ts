/**
 * Standard Risk combat resolution for a single attack throw.
 *
 * One `attack` command resolves exactly one throw (not fight-to-death). The
 * attacker rolls `attackerDice` dice, the defender rolls `min(2, defending
 * armies)`. Dice are sorted descending and compared pairwise; the attacker must
 * strictly beat the defender to inflict a loss (ties favour the defender).
 *
 * Capture note: in a single throw a captured territory always leaves the
 * attacker with zero combat losses (every compared pair the defender loses is a
 * pair the attacker wins). Because an N-dice attack requires N+1 armies in the
 * source, moving `attackerDice` armies into the captured territory always leaves
 * at least one army behind — so `occupyingArmies = attackerDice` is always legal.
 */

import { RULES } from "./map.ts";
import type { Rng } from "./rng.ts";
import { rollDice } from "./rng.ts";

export interface AttackResolution {
  attackerRolls: number[];
  defenderRolls: number[];
  attackerLosses: number;
  defenderLosses: number;
  territoryCaptured: boolean;
  occupyingArmies?: number;
}

const descending = (a: number, b: number): number => b - a;

export function resolveAttack(toArmies: number, attackerDice: number, rng: Rng): AttackResolution {
  const defenderDice = Math.min(RULES.maxDefenderDice, toArmies);
  const attackerRolls = rollDice(rng, attackerDice).toSorted(descending);
  const defenderRolls = rollDice(rng, defenderDice).toSorted(descending);

  let attackerLosses = 0;
  let defenderLosses = 0;
  const pairs = Math.min(attackerRolls.length, defenderRolls.length);
  for (let i = 0; i < pairs; i += 1) {
    if (attackerRolls[i]! > defenderRolls[i]!) defenderLosses += 1;
    else attackerLosses += 1;
  }

  const territoryCaptured = toArmies - defenderLosses <= 0;
  return {
    attackerRolls,
    defenderRolls,
    attackerLosses,
    defenderLosses,
    territoryCaptured,
    occupyingArmies: territoryCaptured ? attackerDice : undefined,
  };
}
