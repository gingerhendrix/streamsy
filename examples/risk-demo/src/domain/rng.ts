/**
 * Injected randomness.
 *
 * The authoritative command service owns all randomness and records resolved
 * outcomes (dice, territory deal, turn order) as facts inside events. Projection
 * and aggregate replay therefore never touch an `Rng`. A tiny deterministic
 * seeded generator is provided for tests and scripted demos; production may
 * inject any implementation, including a CSPRNG.
 */

import { RULES } from "./map.ts";

export interface Rng {
  /** Uniform integer in `[0, bound)`. `bound` must be a positive integer. */
  nextInt(bound: number): number;
}

/** Roll a single `1..dieSides` die. */
export function rollDie(rng: Rng): number {
  return rng.nextInt(RULES.dieSides) + 1;
}

/** Roll `count` dice. */
export function rollDice(rng: Rng, count: number): number[] {
  const rolls: number[] = [];
  for (let i = 0; i < count; i += 1) rolls.push(rollDie(rng));
  return rolls;
}

/** Fisher–Yates shuffle returning a new array; consumes `rng` deterministically. */
export function shuffle<T>(rng: Rng, items: readonly T[]): T[] {
  const out = items.slice();
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = rng.nextInt(i + 1);
    const a = out[i]!;
    const b = out[j]!;
    out[i] = b;
    out[j] = a;
  }
  return out;
}

/**
 * `mulberry32` — a small, fast, fully deterministic PRNG. Given the same seed it
 * always yields the same sequence, which is exactly what the demo's replay and
 * idempotency guarantees require.
 */
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
      const float = ((t ^ (t >>> 14)) >>> 0) / 4294967296;
      return Math.floor(float * bound);
    },
  };
}
