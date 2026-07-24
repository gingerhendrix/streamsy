/**
 * Deterministic integer PRNG for `hex-generator-v1`.
 *
 * Requirements this file exists to satisfy (design spec §3.3):
 *  - explicit 32-bit integer operations only — no `Math.random`, no reliance on
 *    platform floating-point behaviour, no locale-sensitive comparisons;
 *  - independent *named substreams*, so adding a draw to (say) terrain assignment
 *    cannot shift the sequence consumed by land growth. Each substream is seeded
 *    by hashing `"<seed>/<name>"`, so the streams are independent by construction
 *    rather than by careful call ordering.
 *
 * The generator is pure: given the same seed it produces byte-identical output on
 * every platform and every run. Map generation therefore never happens during
 * replay — the seed is provenance, and the recorded snapshot is truth.
 */

/** The named substreams consumed by `hex-generator-v1` and v2 setup. */
export const SUBSTREAMS = [
  "land",
  "territories",
  "continents",
  "terrain",
  "names",
  "setup",
] as const;

export type Substream = (typeof SUBSTREAMS)[number];

export interface IntRng {
  /** Uniform `uint32`. */
  next32(): number;
  /** Uniform integer in `[0, bound)`, unbiased via rejection sampling. */
  nextInt(bound: number): number;
  /** Uniform element of a non-empty array. */
  pick<T>(items: readonly T[]): T;
  /**
   * Element chosen with probability proportional to `weight`. Weights must be
   * non-negative integers and must not sum to zero.
   */
  weightedPick<T>(items: readonly T[], weight: (item: T, index: number) => number): T;
  /** Fisher–Yates shuffle returning a new array. */
  shuffle<T>(items: readonly T[]): T[];
}

/**
 * FNV-1a (32-bit), hashing each UTF-16 code unit as two bytes so the result is
 * independent of string encoding assumptions and of any locale.
 */
export function fnv1a32(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    const code = input.charCodeAt(i);
    hash = Math.imul(hash ^ (code & 0xff), 0x01000193) >>> 0;
    hash = Math.imul(hash ^ ((code >>> 8) & 0xff), 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

/**
 * `splitmix32` — a small integer PRNG using only `imul`, xor, and shifts. Chosen
 * over `mulberry32` here because it never divides by 2^32, so no floating-point
 * value is involved anywhere in map generation.
 */
export function createIntRng(seed32: number): IntRng {
  let state = seed32 >>> 0;

  const next32 = (): number => {
    state = (state + 0x9e3779b9) >>> 0;
    let z = state;
    z = Math.imul(z ^ (z >>> 16), 0x21f0aaad) >>> 0;
    z = Math.imul(z ^ (z >>> 15), 0x735a2d97) >>> 0;
    return (z ^ (z >>> 15)) >>> 0;
  };

  const nextInt = (bound: number): number => {
    if (!Number.isInteger(bound) || bound <= 0) {
      throw new Error(`nextInt bound must be a positive integer, got ${bound}`);
    }
    // Reject the short tail above the largest multiple of `bound` below 2^32 so
    // the result is exactly uniform. 2^32 and every value below it are exactly
    // representable, so this stays integer-precise.
    const limit = 0x100000000 - (0x100000000 % bound);
    let value = next32();
    while (value >= limit) value = next32();
    return value % bound;
  };

  const pick = <T>(items: readonly T[]): T => {
    if (items.length === 0) throw new Error("pick from an empty array");
    return items[nextInt(items.length)]!;
  };

  const weightedPick = <T>(items: readonly T[], weight: (item: T, index: number) => number): T => {
    if (items.length === 0) throw new Error("weightedPick from an empty array");
    let total = 0;
    const weights: number[] = [];
    for (let i = 0; i < items.length; i += 1) {
      const w = weight(items[i]!, i);
      if (!Number.isInteger(w) || w < 0) {
        throw new Error(`weight must be a non-negative integer, got ${w}`);
      }
      weights.push(w);
      total += w;
    }
    if (total <= 0) throw new Error("weightedPick requires a positive total weight");
    let roll = nextInt(total);
    for (let i = 0; i < items.length; i += 1) {
      roll -= weights[i]!;
      if (roll < 0) return items[i]!;
    }
    return items[items.length - 1]!;
  };

  const shuffle = <T>(items: readonly T[]): T[] => {
    const out = items.slice();
    for (let i = out.length - 1; i > 0; i -= 1) {
      const j = nextInt(i + 1);
      const a = out[i]!;
      out[i] = out[j]!;
      out[j] = a;
    }
    return out;
  };

  return { next32, nextInt, pick, weightedPick, shuffle };
}

/** Derive one independent substream from a map seed and a substream name. */
export function createSubstream(seed: string, name: Substream): IntRng {
  return createIntRng(fnv1a32(`${seed}/${name}`));
}

export type Substreams = Readonly<Record<Substream, IntRng>>;

/** Derive every named substream for a seed. Streams are mutually independent. */
export function createSubstreams(seed: string): Substreams {
  const streams = {} as Record<Substream, IntRng>;
  for (const name of SUBSTREAMS) streams[name] = createSubstream(seed, name);
  return streams;
}
