/**
 * Injected randomness.
 *
 * The command service owns all randomness and records resolved outcomes as
 * canonical event facts. Aggregate and projection replay never touch an RNG.
 */

import { RULES } from "./map.ts";

export interface Rng {
  /** Uniform integer in `[0, bound)`. */
  nextInt(bound: number): number;
}

export function rollDie(rng: Rng): number {
  return rng.nextInt(RULES.dieSides) + 1;
}

export function rollDice(rng: Rng, count: number): number[] {
  return Array.from({ length: count }, () => rollDie(rng));
}

export function shuffle<T>(rng: Rng, items: readonly T[]): T[] {
  const out = items.slice();
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = rng.nextInt(i + 1);
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

export function createSeededRng(seed: number): Rng {
  let state = seed >>> 0;
  return {
    nextInt(bound: number): number {
      if (!Number.isInteger(bound) || bound <= 0) {
        throw new Error(`nextInt bound must be a positive integer, got ${bound}`);
      }
      state = (state + 0x6d2b79f5) >>> 0;
      let t = state;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return Math.floor((((t ^ (t >>> 14)) >>> 0) / 4294967296) * bound);
    },
  };
}
