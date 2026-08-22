/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
/**
 * Pointy-top axial hex coordinates.
 *
 * Every playable tile has integer `(q, r)` axial coordinates; the cube coordinate
 * is derived as `(x = q, z = r, y = -q - r)`. Geometry is never stored as browser
 * pixels — SVG positions are computed by the client from `(q, r)` and a
 * client-selected hex radius, so the canonical map snapshot stays device- and
 * renderer-independent.
 *
 * Ordering matters for determinism. The canonical order of any hex collection is
 * *coordinate order* (`q` then `r`), never the lexicographic order of the derived
 * string id and never a locale-sensitive comparison.
 */

/** The six axial neighbour offsets, in canonical order. */
export const AXIAL_NEIGHBORS = [
  [+1, 0],
  [+1, -1],
  [0, -1],
  [-1, 0],
  [-1, +1],
  [0, +1],
] as const;

export interface Axial {
  readonly q: number;
  readonly r: number;
}

/** Stable, coordinate-derived tile identity. */
export function hexId(q: number, r: number): string {
  return `h:${q}:${r}`;
}

export function parseHexId(id: string): Axial {
  const parts = id.split(":");
  if (parts.length !== 3 || parts[0] !== "h") {
    throw new Error(`Malformed hex id: ${id}`);
  }
  const q = Number.parseInt(parts[1]!, 10);
  const r = Number.parseInt(parts[2]!, 10);
  if (!Number.isInteger(q) || !Number.isInteger(r)) {
    throw new Error(`Malformed hex id: ${id}`);
  }
  return { q, r };
}

/** The six coordinate neighbours of `(q, r)`, in `AXIAL_NEIGHBORS` order. */
export function neighborsOf(q: number, r: number): Axial[] {
  return AXIAL_NEIGHBORS.map(([dq, dr]) => ({ q: q + dq, r: r + dr }));
}

/** Canonical coordinate ordering: `q` ascending, then `r` ascending. */
export function compareAxial(a: Axial, b: Axial): number {
  return a.q - b.q || a.r - b.r;
}

/** Sort hex ids into canonical coordinate order. Returns a new array. */
export function sortHexIds(ids: readonly string[]): string[] {
  return ids.toSorted((a, b) => compareAxial(parseHexId(a), parseHexId(b)));
}

/** Hex (cube) distance between two axial coordinates. */
export function hexDistance(a: Axial, b: Axial): number {
  const dq = a.q - b.q;
  const dr = a.r - b.r;
  const ds = -(dq + dr);
  return (Math.abs(dq) + Math.abs(dr) + Math.abs(ds)) / 2;
}

/**
 * Distance from `hex` to a centroid expressed as an unrounded sum over `count`
 * members, scaled by `count` so the comparison stays in exact integer arithmetic.
 *
 * Comparing `scaledCentroidDistance` values is equivalent to comparing true
 * distances to the fractional centroid, but avoids floating point entirely.
 * Returns twice the scaled distance to keep the halving integral.
 */
export function scaledCentroidDistance(
  hex: Axial,
  sumQ: number,
  sumR: number,
  count: number,
): number {
  const dq = hex.q * count - sumQ;
  const dr = hex.r * count - sumR;
  const ds = -(dq + dr);
  return Math.abs(dq) + Math.abs(dr) + Math.abs(ds);
}

/** All hexes within `radius` of the origin, in canonical coordinate order. */
export function hexesWithinRadius(radius: number): Axial[] {
  const out: Axial[] = [];
  for (let q = -radius; q <= radius; q += 1) {
    const lo = Math.max(-radius, -q - radius);
    const hi = Math.min(radius, -q + radius);
    for (let r = lo; r <= hi; r += 1) out.push({ q, r });
  }
  return out.toSorted(compareAxial);
}
