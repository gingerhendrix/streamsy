/**
 * `hex-generator-v1` — the pure, seeded procedural hex-map generator.
 *
 * The generator is a *pure function of its seed*. It runs exactly once per game,
 * inside the start-game command service, and its complete output is recorded in
 * `GameStarted`. Replay reads that snapshot and never calls this module, so a
 * future generator version can never rewrite the history of an existing game.
 * The recorded seed and generator version remain useful for audit and for
 * reproducing the generator's output in tests.
 *
 * Determinism rules obeyed throughout (design spec §3.3):
 *  - randomness comes only from named substreams in `./generator-rng.ts`;
 *  - every collection is sorted into an explicit canonical order *before* any
 *    seeded selection, so no result depends on `Map`/`Set` iteration order;
 *  - comparisons are numeric or code-unit string comparisons — never locale-aware;
 *  - no floating-point arithmetic influences any branch.
 *
 * Generation proceeds as: connected land mask -> country size plan -> territory
 * partition (+ repair) -> geometric adjacency -> continent partition -> terrain
 * -> names and label anchors -> validation. A candidate that fails validation is
 * discarded and retried under `seed + attempt`; exhausting the retry budget
 * rejects game start rather than silently switching algorithms.
 */

import type { Axial } from "./hex.ts";
import {
  compareAxial,
  hexDistance,
  hexId,
  hexesWithinRadius,
  neighborsOf,
  parseHexId,
  scaledCentroidDistance,
  sortHexIds,
} from "./hex.ts";
import type { IntRng } from "./generator-rng.ts";
import { createSubstreams, fnv1a32 } from "./generator-rng.ts";
import { CONTINENT_NAME_POOL, CONTINENT_PALETTES, TERRITORY_NAME_POOL } from "./map-names.ts";
import type {
  ContinentDef,
  GeneratedMap,
  HexTileDef,
  MapProfile,
  Terrain,
  TerritoryDefV2,
} from "./map-v2.ts";
import {
  GENERATOR_VERSION_V2,
  MAP_VERSION_V2,
  RULES_V2,
  continentBonus,
  mapProfileFor,
} from "./map-v2.ts";

/** Number of neighbour-averaging passes applied to elevation and moisture. */
const TERRAIN_SMOOTHING_PASSES = 2;

/** Raw elevation/moisture scores are drawn from `[0, TERRAIN_SCORE_RANGE)`. */
const TERRAIN_SCORE_RANGE = 256;

export class MapGenerationError extends Error {
  readonly seed: string;
  readonly attempts: number;
  readonly problems: readonly string[];

  constructor(seed: string, attempts: number, problems: readonly string[]) {
    super(
      `hex-generator-v1 could not produce a valid map for seed "${seed}" in ${attempts} attempts: ${
        problems.join("; ") || "no candidate reached validation"
      }`,
    );
    this.name = "MapGenerationError";
    this.seed = seed;
    this.attempts = attempts;
    this.problems = problems;
  }
}

export interface GenerateMapRequest {
  readonly seed: string;
  readonly playerCount: number;
}

// ---------------------------------------------------------------------------
// Connectivity helpers
// ---------------------------------------------------------------------------

function isHexSetConnected(ids: readonly string[]): boolean {
  if (ids.length <= 1) return true;
  const set = new Set(ids);
  const start = ids[0]!;
  const seen = new Set([start]);
  const queue = [start];
  for (let head = 0; head < queue.length; head += 1) {
    const { q, r } = parseHexId(queue[head]!);
    for (const n of neighborsOf(q, r)) {
      const nid = hexId(n.q, n.r);
      if (set.has(nid) && !seen.has(nid)) {
        seen.add(nid);
        queue.push(nid);
      }
    }
  }
  return seen.size === set.size;
}

function graphDistances(adjacency: readonly (readonly number[])[], from: number): number[] {
  const dist = Array.from({ length: adjacency.length }, () => -1);
  dist[from] = 0;
  const queue = [from];
  for (let head = 0; head < queue.length; head += 1) {
    const cur = queue[head]!;
    for (const nb of adjacency[cur]!) {
      if (dist[nb] === -1) {
        dist[nb] = dist[cur]! + 1;
        queue.push(nb);
      }
    }
  }
  return dist;
}

function isSubgraphConnected(
  adjacency: readonly (readonly number[])[],
  members: readonly number[],
): boolean {
  if (members.length <= 1) return true;
  const set = new Set(members);
  const start = members[0]!;
  const seen = new Set([start]);
  const queue = [start];
  for (let head = 0; head < queue.length; head += 1) {
    for (const nb of adjacency[queue[head]!]!) {
      if (set.has(nb) && !seen.has(nb)) {
        seen.add(nb);
        queue.push(nb);
      }
    }
  }
  return seen.size === set.size;
}

// ---------------------------------------------------------------------------
// Step 1 — connected land mask
// ---------------------------------------------------------------------------

/** Smallest radius whose hex disc comfortably contains the profile's land target. */
function viewportRadius(target: number): number {
  // Integer form of `discArea(radius) >= target * 1.5`.
  let radius = 1;
  while (2 * (3 * radius * radius + 3 * radius + 1) < 3 * target) radius += 1;
  return radius;
}

/**
 * Grow a connected land mask from the centre of a bounded axial viewport.
 *
 * Frontier candidates are sorted into coordinate order before the seeded pick, and
 * weighted by a deterministic compactness bias — squared count of existing land
 * neighbours, scaled by closeness to the origin — so maps read as blobs rather
 * than long snakes.
 */
function growLand(rng: IntRng, target: number): Axial[] {
  const radius = viewportRadius(target);
  const viewport = new Set(hexesWithinRadius(radius).map((h) => hexId(h.q, h.r)));
  const land = new Set<string>([hexId(0, 0)]);
  const ordered: Axial[] = [{ q: 0, r: 0 }];

  while (land.size < target) {
    const candidates: { hex: Axial; id: string; landNeighbors: number }[] = [];
    const seen = new Set<string>();
    for (const member of ordered) {
      for (const n of neighborsOf(member.q, member.r)) {
        const nid = hexId(n.q, n.r);
        if (land.has(nid) || seen.has(nid) || !viewport.has(nid)) continue;
        seen.add(nid);
        let landNeighbors = 0;
        for (const nn of neighborsOf(n.q, n.r)) {
          if (land.has(hexId(nn.q, nn.r))) landNeighbors += 1;
        }
        candidates.push({ hex: n, id: nid, landNeighbors });
      }
    }
    if (candidates.length === 0) break;
    // Sort before the seeded pick so the choice cannot depend on discovery order.
    const sortedCandidates = candidates.toSorted((a, b) => compareAxial(a.hex, b.hex));

    const chosen = rng.weightedPick(sortedCandidates, (c) => {
      const closeness = radius + 1 - hexDistance(c.hex, { q: 0, r: 0 });
      return c.landNeighbors * c.landNeighbors * Math.max(1, closeness);
    });
    land.add(chosen.id);
    ordered.push(chosen.hex);
  }

  return ordered.toSorted(compareAxial);
}

// ---------------------------------------------------------------------------
// Step 2 — country size plan
// ---------------------------------------------------------------------------

/** Target country sizes in `[min, max]`, deterministically rebalanced to sum to `total`. */
function planTerritorySizes(rng: IntRng, count: number, total: number): number[] | null {
  const min = RULES_V2.minTerritoryHexes;
  const max = RULES_V2.maxTerritoryHexes;
  if (count * min > total || count * max < total) return null;

  const sizes: number[] = [];
  for (let i = 0; i < count; i += 1) sizes.push(min + rng.nextInt(max - min + 1));

  let sum = sizes.reduce((a, b) => a + b, 0);
  while (sum !== total) {
    const grow = sum < total;
    const eligible: number[] = [];
    for (let i = 0; i < count; i += 1) {
      if (grow ? sizes[i]! < max : sizes[i]! > min) eligible.push(i);
    }
    if (eligible.length === 0) return null;
    const index = rng.pick(eligible);
    sizes[index] = sizes[index]! + (grow ? 1 : -1);
    sum += grow ? 1 : -1;
  }
  return sizes;
}

// ---------------------------------------------------------------------------
// Step 3 — territory partition
// ---------------------------------------------------------------------------

/** Farthest-point sampling: well-spaced seeds, ties broken by coordinate order. */
function chooseSpacedHexSeeds(rng: IntRng, land: readonly Axial[], count: number): Axial[] {
  const seeds: Axial[] = [rng.pick(land)];
  while (seeds.length < count) {
    let best = -1;
    let tied: Axial[] = [];
    for (const hex of land) {
      if (seeds.some((s) => s.q === hex.q && s.r === hex.r)) continue;
      let minDistance = Number.POSITIVE_INFINITY;
      for (const s of seeds) minDistance = Math.min(minDistance, hexDistance(hex, s));
      if (minDistance > best) {
        best = minDistance;
        tied = [hex];
      } else if (minDistance === best) {
        tied.push(hex);
      }
    }
    if (tied.length === 0) break;
    seeds.push(rng.pick(tied));
  }
  return seeds;
}

interface Partition {
  /** Hex ids per territory index, coordinate-sorted. */
  readonly groups: string[][];
  /** Territory index per hex id. */
  readonly owner: Map<string, number>;
}

function partitionTerritories(
  rng: IntRng,
  land: readonly Axial[],
  sizes: readonly number[],
): Partition | null {
  const landIds = land.map((h) => hexId(h.q, h.r));
  const landSet = new Set(landIds);
  const seeds = chooseSpacedHexSeeds(rng, land, sizes.length);
  if (seeds.length !== sizes.length) return null;

  const owner = new Map<string, number>();
  const groups: string[][] = seeds.map((s, i) => {
    const id = hexId(s.q, s.r);
    owner.set(id, i);
    return [id];
  });
  if (owner.size !== sizes.length) return null; // duplicate seeds

  // Multi-source growth: each under-target territory takes one frontier hex per
  // round, biased toward hexes that already touch it (keeps countries compact).
  let progress = true;
  while (progress) {
    progress = false;
    for (let i = 0; i < groups.length; i += 1) {
      if (groups[i]!.length >= sizes[i]!) continue;
      const frontier: { id: string; hex: Axial; touching: number }[] = [];
      const seen = new Set<string>();
      for (const memberId of groups[i]!) {
        const { q, r } = parseHexId(memberId);
        for (const n of neighborsOf(q, r)) {
          const nid = hexId(n.q, n.r);
          if (!landSet.has(nid) || owner.has(nid) || seen.has(nid)) continue;
          seen.add(nid);
          let touching = 0;
          for (const nn of neighborsOf(n.q, n.r)) {
            if (owner.get(hexId(nn.q, nn.r)) === i) touching += 1;
          }
          frontier.push({ id: nid, hex: n, touching });
        }
      }
      if (frontier.length === 0) continue;
      const sortedFrontier = frontier.toSorted((a, b) => compareAxial(a.hex, b.hex));
      const chosen = rng.weightedPick(sortedFrontier, (f) => f.touching * f.touching);
      owner.set(chosen.id, i);
      groups[i]!.push(chosen.id);
      progress = true;
    }
  }

  // Any land the growth phase could not reach joins an adjacent territory,
  // preferring the smallest still-under-max neighbour.
  let leftovers = landIds.filter((id) => !owner.has(id));
  while (leftovers.length > 0) {
    let assignedAny = false;
    for (const id of leftovers) {
      const { q, r } = parseHexId(id);
      const neighbourIndices = new Set<number>();
      for (const n of neighborsOf(q, r)) {
        const index = owner.get(hexId(n.q, n.r));
        if (index !== undefined) neighbourIndices.add(index);
      }
      if (neighbourIndices.size === 0) continue;
      const options = [...neighbourIndices].toSorted((a, b) => a - b);
      const underMax = options.filter((i) => groups[i]!.length < RULES_V2.maxTerritoryHexes);
      const pool = underMax.length > 0 ? underMax : options;
      let best = pool[0]!;
      for (const i of pool) {
        if (groups[i]!.length < groups[best]!.length) best = i;
      }
      owner.set(id, best);
      groups[best]!.push(id);
      assignedAny = true;
    }
    const remaining = landIds.filter((id) => !owner.has(id));
    if (!assignedAny || remaining.length === leftovers.length) return null;
    leftovers = remaining;
  }

  if (!repairUndersizedTerritories(groups, owner)) return null;

  for (const group of groups) {
    const sorted = sortHexIds(group);
    group.length = 0;
    group.push(...sorted);
  }
  return { groups, owner };
}

/**
 * Transfer boundary hexes into undersized territories, never disconnecting the
 * donor and never pushing the donor below the minimum. Deterministic: candidates
 * are evaluated in coordinate order and the first legal transfer wins.
 */
function repairUndersizedTerritories(groups: string[][], owner: Map<string, number>): boolean {
  const min = RULES_V2.minTerritoryHexes;
  for (let i = 0; i < groups.length; i += 1) {
    let guard = 0;
    while (groups[i]!.length < min) {
      guard += 1;
      if (guard > min * groups.length) return false;

      const candidates: string[] = [];
      for (const memberId of groups[i]!) {
        const { q, r } = parseHexId(memberId);
        for (const n of neighborsOf(q, r)) {
          const nid = hexId(n.q, n.r);
          const donor = owner.get(nid);
          if (donor === undefined || donor === i) continue;
          if (groups[donor]!.length <= min) continue;
          const remainder = groups[donor]!.filter((h) => h !== nid);
          if (!isHexSetConnected(remainder)) continue;
          candidates.push(nid);
        }
      }
      if (candidates.length === 0) return false;

      const chosen = sortHexIds(candidates)[0]!;
      const donor = owner.get(chosen)!;
      groups[donor] = groups[donor]!.filter((h) => h !== chosen);
      groups[i]!.push(chosen);
      owner.set(chosen, i);
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Step 4 — territory adjacency, derived from geometry
// ---------------------------------------------------------------------------

/** Undirected territory adjacency implied by physical hex contact. */
function deriveTerritoryAdjacency(
  groups: readonly (readonly string[])[],
  owner: ReadonlyMap<string, number>,
): number[][] {
  const sets = groups.map(() => new Set<number>());
  for (const [id, index] of owner) {
    const { q, r } = parseHexId(id);
    for (const n of neighborsOf(q, r)) {
      const other = owner.get(hexId(n.q, n.r));
      if (other === undefined || other === index) continue;
      sets[index]!.add(other);
      sets[other]!.add(index);
    }
  }
  return sets.map((s) => [...s].toSorted((a, b) => a - b));
}

// ---------------------------------------------------------------------------
// Step 5 — continent partition
// ---------------------------------------------------------------------------

function partitionContinents(
  rng: IntRng,
  adjacency: readonly (readonly number[])[],
  continentCount: number,
): number[][] | null {
  const territoryCount = adjacency.length;
  const base = Math.floor(territoryCount / continentCount);
  const remainder = territoryCount % continentCount;
  const targets = Array.from({ length: continentCount }, (_, i) => base + (i < remainder ? 1 : 0));
  if (targets.some((t) => t < RULES_V2.minContinentTerritories)) return null;

  // Farthest-point seeds on the territory graph.
  const seeds: number[] = [rng.nextInt(territoryCount)];
  while (seeds.length < continentCount) {
    const distanceSets = seeds.map((s) => graphDistances(adjacency, s));
    let best = -1;
    let tied: number[] = [];
    for (let t = 0; t < territoryCount; t += 1) {
      if (seeds.includes(t)) continue;
      let minDistance = Number.POSITIVE_INFINITY;
      for (const dist of distanceSets) {
        const d = dist[t]!;
        if (d >= 0) minDistance = Math.min(minDistance, d);
      }
      if (!Number.isFinite(minDistance)) continue;
      if (minDistance > best) {
        best = minDistance;
        tied = [t];
      } else if (minDistance === best) {
        tied.push(t);
      }
    }
    if (tied.length === 0) return null;
    seeds.push(rng.pick(tied));
  }

  const group = new Map<number, number>();
  const members: number[][] = seeds.map((s, i) => {
    group.set(s, i);
    return [s];
  });
  if (group.size !== continentCount) return null;

  let progress = true;
  while (progress) {
    progress = false;
    for (let i = 0; i < continentCount; i += 1) {
      if (members[i]!.length >= targets[i]!) continue;
      const frontier = new Set<number>();
      for (const member of members[i]!) {
        for (const nb of adjacency[member]!) if (!group.has(nb)) frontier.add(nb);
      }
      if (frontier.size === 0) continue;
      const options = [...frontier].toSorted((a, b) => a - b);
      const chosen = rng.weightedPick(options, (t) => {
        let touching = 0;
        for (const nb of adjacency[t]!) if (group.get(nb) === i) touching += 1;
        return touching * touching;
      });
      group.set(chosen, i);
      members[i]!.push(chosen);
      progress = true;
    }
  }

  // Unreached territories join the smallest adjacent continent.
  let leftovers = Array.from({ length: territoryCount }, (_, i) => i).filter((t) => !group.has(t));
  while (leftovers.length > 0) {
    let assignedAny = false;
    for (const t of leftovers) {
      const options = new Set<number>();
      for (const nb of adjacency[t]!) {
        const g = group.get(nb);
        if (g !== undefined) options.add(g);
      }
      if (options.size === 0) continue;
      const sorted = [...options].toSorted((a, b) => a - b);
      let best = sorted[0]!;
      for (const i of sorted) if (members[i]!.length < members[best]!.length) best = i;
      group.set(t, best);
      members[best]!.push(t);
      assignedAny = true;
    }
    const remaining = Array.from({ length: territoryCount }, (_, i) => i).filter(
      (t) => !group.has(t),
    );
    if (!assignedAny || remaining.length === leftovers.length) return null;
    leftovers = remaining;
  }

  return members.map((list) => list.toSorted((a, b) => a - b));
}

// ---------------------------------------------------------------------------
// Step 6 — terrain (visual only)
// ---------------------------------------------------------------------------

function smoothScores(
  land: readonly Axial[],
  scores: ReadonlyMap<string, number>,
): Map<string, number> {
  const next = new Map<string, number>();
  for (const hex of land) {
    const id = hexId(hex.q, hex.r);
    // Self counts double so a tile keeps its own character while blending.
    let sum = scores.get(id)! * 2;
    let count = 2;
    for (const n of neighborsOf(hex.q, hex.r)) {
      const value = scores.get(hexId(n.q, n.r));
      if (value === undefined) continue;
      sum += value;
      count += 1;
    }
    next.set(id, Math.floor(sum / count));
  }
  return next;
}

/**
 * Assign terrain from smoothed elevation and moisture fields.
 *
 * Classification is by *rank* within the smoothed field rather than by absolute
 * thresholds: smoothing pulls values toward the mean, so fixed cutoffs would make
 * extreme terrains vanish on some seeds. Ranking a smooth field still yields
 * contiguous patches that cross country borders naturally, while guaranteeing
 * every map shows a readable mix.
 */
function assignTerrain(rng: IntRng, land: readonly Axial[]): Map<string, Terrain> {
  let elevation = new Map<string, number>();
  let moisture = new Map<string, number>();
  for (const hex of land) {
    const id = hexId(hex.q, hex.r);
    elevation.set(id, rng.nextInt(TERRAIN_SCORE_RANGE));
    moisture.set(id, rng.nextInt(TERRAIN_SCORE_RANGE));
  }
  for (let pass = 0; pass < TERRAIN_SMOOTHING_PASSES; pass += 1) {
    elevation = smoothScores(land, elevation);
    moisture = smoothScores(land, moisture);
  }

  const rankBy = (scores: ReadonlyMap<string, number>, ids: readonly string[]): string[] =>
    ids.toSorted(
      (a, b) => scores.get(a)! - scores.get(b)! || compareAxial(parseHexId(a), parseHexId(b)),
    );

  const allIds = land.map((h) => hexId(h.q, h.r));
  const byElevation = rankBy(elevation, allIds);
  const total = allIds.length;
  const mountainCount = Math.max(1, Math.round((total * 12) / 100));
  const hillCount = Math.max(1, Math.round((total * 18) / 100));

  const terrain = new Map<string, Terrain>();
  const highest = byElevation.slice(total - mountainCount);
  const hills = byElevation.slice(total - mountainCount - hillCount, total - mountainCount);
  for (const id of highest) terrain.set(id, "mountains");
  for (const id of hills) terrain.set(id, "hills");

  const lowland = byElevation.slice(0, total - mountainCount - hillCount);
  const byMoisture = rankBy(moisture, lowland);
  const desertCount = Math.max(1, Math.round((byMoisture.length * 25) / 100));
  const forestCount = Math.max(1, Math.round((byMoisture.length * 30) / 100));
  for (let i = 0; i < byMoisture.length; i += 1) {
    const id = byMoisture[i]!;
    if (i < desertCount) terrain.set(id, "desert");
    else if (i >= byMoisture.length - forestCount) terrain.set(id, "forest");
    else terrain.set(id, "plains");
  }

  return terrain;
}

// ---------------------------------------------------------------------------
// Step 7 — assembly, names, label anchors
// ---------------------------------------------------------------------------

function territoryLabelAnchor(hexIds: readonly string[]): Axial {
  const coords = hexIds.map(parseHexId);
  let sumQ = 0;
  let sumR = 0;
  for (const c of coords) {
    sumQ += c.q;
    sumR += c.r;
  }
  let best = coords[0]!;
  let bestDistance = scaledCentroidDistance(best, sumQ, sumR, coords.length);
  for (const c of coords.slice(1)) {
    const distance = scaledCentroidDistance(c, sumQ, sumR, coords.length);
    if (distance < bestDistance || (distance === bestDistance && compareAxial(c, best) < 0)) {
      best = c;
      bestDistance = distance;
    }
  }
  return { q: best.q, r: best.r };
}

function territoryIdFor(index: number): string {
  return `t:${String(index).padStart(2, "0")}`;
}

function continentIdFor(index: number): string {
  return `c:${index}`;
}

// ---------------------------------------------------------------------------
// Step 8 — validation
// ---------------------------------------------------------------------------

/** Structural invariants every generated map must satisfy. Returns problems found. */
export function validateGeneratedMap(map: GeneratedMap, profile: MapProfile): string[] {
  const problems: string[] = [];

  if (map.tiles.length !== profile.hexes) {
    problems.push(`expected ${profile.hexes} tiles, got ${map.tiles.length}`);
  }
  if (map.territories.length !== profile.territories) {
    problems.push(`expected ${profile.territories} territories, got ${map.territories.length}`);
  }
  if (map.continents.length !== profile.continents) {
    problems.push(`expected ${profile.continents} continents, got ${map.continents.length}`);
  }

  const allHexIds = map.tiles.map((t) => t.id);
  if (new Set(allHexIds).size !== allHexIds.length) problems.push("duplicate tile ids");
  if (!isHexSetConnected(allHexIds)) problems.push("land mask is not connected");

  const territoryIds = new Set(map.territories.map((t) => t.id));
  const indexOf = new Map(map.territories.map((t, i) => [t.id, i]));
  const adjacency = map.territories.map((t) =>
    t.adjacentTerritoryIds.map((id) => indexOf.get(id)).filter((i): i is number => i !== undefined),
  );

  const seenHexes = new Set<string>();
  for (const territory of map.territories) {
    const size = territory.hexIds.length;
    if (size < RULES_V2.minTerritoryHexes || size > RULES_V2.maxTerritoryHexes) {
      problems.push(
        `${territory.id} has ${size} hexes, outside ${RULES_V2.minTerritoryHexes}..${RULES_V2.maxTerritoryHexes}`,
      );
    }
    if (!isHexSetConnected(territory.hexIds)) problems.push(`${territory.id} is not connected`);
    if (territory.adjacentTerritoryIds.length === 0) problems.push(`${territory.id} is isolated`);
    for (const hex of territory.hexIds) {
      if (seenHexes.has(hex)) problems.push(`hex ${hex} claimed by more than one territory`);
      seenHexes.add(hex);
    }
    for (const other of territory.adjacentTerritoryIds) {
      if (!territoryIds.has(other)) {
        problems.push(`${territory.id} references unknown neighbour ${other}`);
        continue;
      }
      const back = map.territories[indexOf.get(other)!]!;
      if (!back.adjacentTerritoryIds.includes(territory.id)) {
        problems.push(`adjacency ${territory.id}->${other} is not symmetric`);
      }
    }
    if (!territory.hexIds.includes(hexId(territory.labelAnchor.q, territory.labelAnchor.r))) {
      problems.push(`${territory.id} label anchor is not a member hex`);
    }
  }
  if (seenHexes.size !== allHexIds.length) {
    problems.push("territories do not cover the land mask exactly");
  }

  if (
    map.territories.length > 0 &&
    !isSubgraphConnected(
      adjacency,
      adjacency.map((_, i) => i),
    )
  ) {
    problems.push("territory adjacency graph is not connected");
  }

  const assignedTerritories = new Set<string>();
  for (const continent of map.continents) {
    if (continent.territoryIds.length < RULES_V2.minContinentTerritories) {
      problems.push(
        `${continent.id} has ${continent.territoryIds.length} territories, fewer than ${RULES_V2.minContinentTerritories}`,
      );
    }
    if (continent.reinforcementBonus !== continentBonus(continent.territoryIds.length)) {
      problems.push(`${continent.id} bonus does not match its territory count`);
    }
    const memberIndices: number[] = [];
    for (const id of continent.territoryIds) {
      if (assignedTerritories.has(id)) problems.push(`${id} belongs to more than one continent`);
      assignedTerritories.add(id);
      const index = indexOf.get(id);
      if (index === undefined) {
        problems.push(`${continent.id} references unknown territory ${id}`);
        continue;
      }
      memberIndices.push(index);
      if (map.territories[index]!.continentId !== continent.id) {
        problems.push(`${id} disagrees about its continent`);
      }
    }
    if (!isSubgraphConnected(adjacency, memberIndices)) {
      problems.push(`${continent.id} is not connected`);
    }
  }
  if (assignedTerritories.size !== map.territories.length) {
    problems.push("continents do not cover every territory exactly");
  }

  const names = map.territories.map((t) => t.name);
  if (new Set(names).size !== names.length) problems.push("duplicate territory names");
  const continentNames = map.continents.map((c) => c.name);
  if (new Set(continentNames).size !== continentNames.length) {
    problems.push("duplicate continent names");
  }

  return problems;
}

// ---------------------------------------------------------------------------
// Generation entry points
// ---------------------------------------------------------------------------

function attemptGenerate(
  recordedSeed: string,
  attemptSeed: string,
  profile: MapProfile,
): { map: GeneratedMap; problems: string[] } | null {
  const streams = createSubstreams(attemptSeed);

  const land = growLand(streams.land, profile.hexes);
  if (land.length !== profile.hexes) return null;

  const sizes = planTerritorySizes(streams.territories, profile.territories, profile.hexes);
  if (!sizes) return null;

  const partition = partitionTerritories(streams.territories, land, sizes);
  if (!partition) return null;

  const adjacency = deriveTerritoryAdjacency(partition.groups, partition.owner);
  const continentMembers = partitionContinents(streams.continents, adjacency, profile.continents);
  if (!continentMembers) return null;

  const terrain = assignTerrain(streams.terrain, land);

  // Canonical territory ids follow geometry: order groups by their lowest member
  // hex, so ids are stable for a given partition regardless of growth order.
  const order = partition.groups
    .map((group, index) => ({ index, first: parseHexId(sortHexIds(group)[0]!) }))
    .toSorted((a, b) => compareAxial(a.first, b.first))
    .map((entry) => entry.index);
  const idByIndex = new Map<number, string>();
  order.forEach((groupIndex, position) => idByIndex.set(groupIndex, territoryIdFor(position)));

  // Continent ids follow their lowest member territory id.
  const continentOrder = continentMembers
    .map((members, index) => ({
      index,
      firstId: members.map((m) => idByIndex.get(m)!).toSorted()[0]!,
    }))
    .toSorted((a, b) => (a.firstId < b.firstId ? -1 : a.firstId > b.firstId ? 1 : 0))
    .map((entry) => entry.index);
  const continentIdByIndex = new Map<number, string>();
  continentOrder.forEach((memberIndex, position) =>
    continentIdByIndex.set(memberIndex, continentIdFor(position)),
  );
  const continentOfTerritory = new Map<number, string>();
  continentMembers.forEach((members, index) => {
    for (const t of members) continentOfTerritory.set(t, continentIdByIndex.get(index)!);
  });

  const territoryNames = streams.names.shuffle(TERRITORY_NAME_POOL).slice(0, profile.territories);
  const continentNames = streams.names.shuffle(CONTINENT_NAME_POOL).slice(0, profile.continents);
  if (
    territoryNames.length !== profile.territories ||
    continentNames.length !== profile.continents
  ) {
    return null;
  }

  const territories: TerritoryDefV2[] = order.map((groupIndex, position) => {
    const hexIds = sortHexIds(partition.groups[groupIndex]!);
    return {
      id: territoryIdFor(position),
      name: territoryNames[position]!,
      continentId: continentOfTerritory.get(groupIndex)!,
      hexIds,
      adjacentTerritoryIds: adjacency[groupIndex]!.map((i) => idByIndex.get(i)!).toSorted(),
      labelAnchor: territoryLabelAnchor(hexIds),
    };
  });

  const continents: ContinentDef[] = continentOrder.map((memberIndex, position) => {
    const territoryIds = continentMembers[memberIndex]!.map((t) => idByIndex.get(t)!).toSorted();
    const palette = CONTINENT_PALETTES[position % CONTINENT_PALETTES.length]!;
    return {
      id: continentIdFor(position),
      name: continentNames[position]!,
      territoryIds,
      reinforcementBonus: continentBonus(territoryIds.length),
      palette: { hue: palette.hue, pattern: palette.pattern },
    };
  });

  const tiles: HexTileDef[] = land.map((hex) => {
    const id = hexId(hex.q, hex.r);
    return {
      id,
      q: hex.q,
      r: hex.r,
      territoryId: idByIndex.get(partition.owner.get(id)!)!,
      terrain: terrain.get(id)!,
    };
  });

  const qs = land.map((h) => h.q);
  const rs = land.map((h) => h.r);
  const map: GeneratedMap = {
    mapVersion: MAP_VERSION_V2,
    generatorVersion: GENERATOR_VERSION_V2,
    seed: recordedSeed,
    widthHint: Math.max(...qs) - Math.min(...qs) + 1,
    heightHint: Math.max(...rs) - Math.min(...rs) + 1,
    tiles,
    territories,
    continents,
  };

  const problems = validateGeneratedMap(map, profile);
  return { map, problems };
}

/**
 * Generate the canonical map for a game. Pure in `(seed, playerCount)`.
 *
 * A candidate that fails validation is retried under a deterministically derived
 * attempt seed. Exhausting the budget throws {@link MapGenerationError}, which the
 * start-game command service surfaces as a rejected start — never a silent
 * fallback to a different algorithm.
 */
export function generateHexMap(request: GenerateMapRequest): GeneratedMap {
  const profile = mapProfileFor(request.playerCount);
  const problems: string[] = [];

  for (let attempt = 0; attempt < RULES_V2.maxGenerationAttempts; attempt += 1) {
    const attemptSeed = attempt === 0 ? request.seed : `${request.seed}#${attempt}`;
    const candidate = attemptGenerate(request.seed, attemptSeed, profile);
    if (candidate && candidate.problems.length === 0) return candidate.map;
    if (candidate) problems.push(`attempt ${attempt}: ${candidate.problems.join(", ")}`);
  }

  throw new MapGenerationError(request.seed, RULES_V2.maxGenerationAttempts, problems);
}

// ---------------------------------------------------------------------------
// Snapshot hashing
// ---------------------------------------------------------------------------

/** Canonical JSON: object keys in code-unit order, so the encoding is stable. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`;
}

/**
 * Stable 64-bit hash of a map snapshot, as 16 lowercase hex characters. Used by
 * known-seed tests to pin generator output byte-for-byte.
 */
export function hashGeneratedMap(map: GeneratedMap): string {
  const encoded = canonicalJson(map);
  const high = fnv1a32(encoded);
  const low = fnv1a32(`${encoded}|${encoded.length}`);
  return high.toString(16).padStart(8, "0") + low.toString(16).padStart(8, "0");
}
