import { describe, expect, it } from "vitest";

import { compareAxial, hexId, hexesWithinRadius, neighborsOf, parseHexId } from "./hex.ts";
import { SUBSTREAMS, createIntRng, createSubstream, createSubstreams } from "./generator-rng.ts";
import {
  MapGenerationError,
  canonicalJson,
  generateHexMap,
  hashGeneratedMap,
  validateGeneratedMap,
} from "./hex-generator.ts";
import type { GeneratedMap } from "./map.ts";
import {
  GENERATOR_VERSION,
  MAP_PROFILES,
  MAP_VERSION,
  RULES,
  TERRAIN_TYPES,
  continentBonus,
  generateMapSeed,
  indexMap,
  mapProfileFor,
  maxContinentTerritories,
} from "./map.ts";
import { createSeededRng } from "./rng.ts";

// ---------------------------------------------------------------------------
// Axial coordinate primitives
// ---------------------------------------------------------------------------

describe("axial hex coordinates", () => {
  it("round-trips tile identity through the coordinate-derived id", () => {
    for (const { q, r } of hexesWithinRadius(3)) {
      expect(parseHexId(hexId(q, r))).toEqual({ q, r });
    }
  });

  it("has symmetric neighbours", () => {
    for (const hex of hexesWithinRadius(2)) {
      for (const n of neighborsOf(hex.q, hex.r)) {
        const back = neighborsOf(n.q, n.r);
        expect(back.some((b) => b.q === hex.q && b.r === hex.r)).toBe(true);
      }
    }
  });

  it("counts a hex disc as 3r^2 + 3r + 1", () => {
    for (let radius = 0; radius <= 5; radius += 1) {
      expect(hexesWithinRadius(radius)).toHaveLength(3 * radius * radius + 3 * radius + 1);
    }
  });
});

// ---------------------------------------------------------------------------
// PRNG
// ---------------------------------------------------------------------------

describe("generator rng", () => {
  it("is reproducible for a seed and independent across substreams", () => {
    const a = createSubstreams("seed-one");
    const b = createSubstreams("seed-one");
    for (const name of SUBSTREAMS) {
      const left = Array.from({ length: 8 }, () => a[name].next32());
      const right = Array.from({ length: 8 }, () => b[name].next32());
      expect(left).toEqual(right);
    }

    // Draining one substream must not shift another.
    const fresh = createSubstreams("seed-one");
    for (let i = 0; i < 50; i += 1) fresh.land.next32();
    expect(fresh.terrain.next32()).toBe(createSubstream("seed-one", "terrain").next32());
  });

  it("produces uniform bounded integers without floating point", () => {
    const rng = createIntRng(12345);
    const counts = Array.from({ length: 6 }, () => 0);
    for (let i = 0; i < 60_000; i += 1) counts[rng.nextInt(6)]! += 1;
    for (const count of counts) {
      expect(count).toBeGreaterThan(9_000);
      expect(count).toBeLessThan(11_000);
    }
  });

  it("rejects invalid bounds and empty selections", () => {
    const rng = createIntRng(1);
    expect(() => rng.nextInt(0)).toThrow();
    expect(() => rng.nextInt(1.5)).toThrow();
    expect(() => rng.pick([])).toThrow();
    expect(() => rng.weightedPick([1, 2], () => 0)).toThrow();
  });

  it("shuffles as a permutation", () => {
    const rng = createIntRng(99);
    const input = Array.from({ length: 20 }, (_, i) => i);
    const out = rng.shuffle(input);
    expect(out).not.toEqual(input);
    expect(out.toSorted((a, b) => a - b)).toEqual(input);
  });

  it("draws a 128-bit map seed as 32 hex characters", () => {
    const seed = generateMapSeed(createSeededRng(7));
    expect(seed).toMatch(/^[0-9a-f]{32}$/);
    expect(generateMapSeed(createSeededRng(7))).toBe(seed);
    expect(generateMapSeed(createSeededRng(8))).not.toBe(seed);
  });
});

// ---------------------------------------------------------------------------
// Known-seed snapshots
// ---------------------------------------------------------------------------

/**
 * Pinned hashes of `hex-generator-v2` output. These are the byte-stability
 * guarantee: any change to the algorithm, the name pools, or the normalization
 * order must break these and force a new generator version.
 */
const KNOWN_SEED_HASHES: ReadonlyArray<{ seed: string; players: number; hash: string }> = [
  { seed: "seed-alpha", players: 2, hash: "15ab0101e9b0c9f0" },
  { seed: "seed-alpha", players: 3, hash: "1374c408e270800b" },
  { seed: "seed-alpha", players: 4, hash: "e19f3b85c8a95e35" },
];

describe("known-seed map snapshots", () => {
  it.each(KNOWN_SEED_HASHES)(
    "pins seed $seed at $players players to $hash",
    ({ seed, players, hash }) => {
      expect(hashGeneratedMap(generateHexMap({ seed, playerCount: players }))).toBe(hash);
    },
  );

  it("is byte-stable across repeated generation", () => {
    const first = generateHexMap({ seed: "byte-stable", playerCount: 3 });
    const second = generateHexMap({ seed: "byte-stable", playerCount: 3 });
    expect(canonicalJson(second)).toBe(canonicalJson(first));
  });

  it("hashes independently of key insertion order", () => {
    const map = generateHexMap({ seed: "key-order", playerCount: 2 });
    const reordered = reverseKeyOrder(map) as GeneratedMap;
    // Raw JSON differs (keys really were re-inserted in the opposite order)...
    expect(JSON.stringify(reordered)).not.toBe(JSON.stringify(map));
    // ...but the canonical encoding and therefore the hash do not.
    expect(canonicalJson(reordered)).toBe(canonicalJson(map));
    expect(hashGeneratedMap(reordered)).toBe(hashGeneratedMap(map));
  });

  it("records provenance without depending on it for replay", () => {
    const map = generateHexMap({ seed: "provenance", playerCount: 2 });
    expect(map.seed).toBe("provenance");
    expect(map.mapVersion).toBe(MAP_VERSION);
    expect(map.generatorVersion).toBe(GENERATOR_VERSION);
  });
});

// ---------------------------------------------------------------------------
// Structural invariants across a large seed sample
// ---------------------------------------------------------------------------

const SAMPLE_SEEDS = Array.from({ length: 40 }, (_, i) => `sample-${i}`);

describe.each(MAP_PROFILES)("profile: $players players", (profile) => {
  const maps = SAMPLE_SEEDS.map((seed) => generateHexMap({ seed, playerCount: profile.players }));

  it("matches the profile's hex, territory, and continent counts", () => {
    for (const map of maps) {
      expect(map.tiles).toHaveLength(profile.hexes);
      expect(map.territories).toHaveLength(profile.territories);
      expect(map.continents).toHaveLength(profile.continents);
    }
  });

  it("passes the full structural validator on every sampled seed", () => {
    for (const map of maps) {
      expect(validateGeneratedMap(map, profile)).toEqual([]);
    }
  });

  it("keeps every country connected and within the size bounds", () => {
    for (const map of maps) {
      for (const territory of map.territories) {
        expect(territory.hexIds.length).toBeGreaterThanOrEqual(RULES.minTerritoryHexes);
        expect(territory.hexIds.length).toBeLessThanOrEqual(RULES.maxTerritoryHexes);
        expect(isConnectedHexes(territory.hexIds)).toBe(true);
      }
    }
  });

  it("derives adjacency from hex contact only", () => {
    for (const map of maps) {
      const territoryOfHex = new Map(map.tiles.map((t) => [t.id, t.territoryId]));
      const expected = new Map(map.territories.map((t) => [t.id, new Set<string>()]));
      for (const tile of map.tiles) {
        for (const n of neighborsOf(tile.q, tile.r)) {
          const other = territoryOfHex.get(hexId(n.q, n.r));
          if (other === undefined || other === tile.territoryId) continue;
          expected.get(tile.territoryId)!.add(other);
          expected.get(other)!.add(tile.territoryId);
        }
      }
      for (const territory of map.territories) {
        expect(territory.adjacentTerritoryIds).toEqual([...expected.get(territory.id)!].toSorted());
      }
    }
  });

  it("keeps continents connected, large enough, and correctly scored", () => {
    for (const map of maps) {
      const index = indexMap(map);
      for (const continent of map.continents) {
        expect(continent.territoryIds.length).toBeGreaterThanOrEqual(RULES.minContinentTerritories);
        expect(continent.reinforcementBonus).toBe(continentBonus(continent.territoryIds.length));
        expect(continent.reinforcementBonus).toBeGreaterThanOrEqual(RULES.minContinentBonus);
        expect(continent.territoryIds.length).toBeLessThanOrEqual(maxContinentTerritories(profile));

        const members = new Set(continent.territoryIds);
        const start = continent.territoryIds[0]!;
        const seen = new Set([start]);
        const queue = [start];
        for (let head = 0; head < queue.length; head += 1) {
          for (const nb of index.territoryById.get(queue[head]!)!.adjacentTerritoryIds) {
            if (members.has(nb) && !seen.has(nb)) {
              seen.add(nb);
              queue.push(nb);
            }
          }
        }
        expect(seen.size).toBe(members.size);
      }
    }
  });

  // The bonus is paid for territory count, so continents that all hold the same
  // number of countries make the choice of which to hold arbitrary. Every seed must
  // offer a continent worth more than another.
  it("gives every map a more and a less valuable continent", () => {
    for (const map of maps) {
      const bonuses = map.continents.map((continent) => continent.reinforcementBonus);
      expect(Math.max(...bonuses)).toBeGreaterThan(Math.min(...bonuses));
    }
  });

  it("partitions the land exactly once across territories and continents", () => {
    for (const map of maps) {
      const hexes = map.territories.flatMap((t) => [...t.hexIds]);
      expect(new Set(hexes).size).toBe(profile.hexes);
      expect(hexes).toHaveLength(profile.hexes);

      const territories = map.continents.flatMap((c) => [...c.territoryIds]);
      expect(new Set(territories).size).toBe(profile.territories);
      expect(territories).toHaveLength(profile.territories);
    }
  });

  it("emits normalized, stably ordered collections", () => {
    for (const map of maps) {
      expect(map.tiles.map((t) => ({ q: t.q, r: t.r }))).toEqual(
        map.tiles.map((t) => ({ q: t.q, r: t.r })).toSorted(compareAxial),
      );
      expect(map.territories.map((t) => t.id)).toEqual(map.territories.map((t) => t.id).toSorted());
      expect(map.continents.map((c) => c.id)).toEqual(map.continents.map((c) => c.id).toSorted());
      for (const territory of map.territories) {
        expect(territory.adjacentTerritoryIds).toEqual(
          [...territory.adjacentTerritoryIds].toSorted(),
        );
        expect(territory.hexIds).toEqual(
          [...territory.hexIds].toSorted((a, b) => compareAxial(parseHexId(a), parseHexId(b))),
        );
      }
    }
  });

  it("labels every country uniquely and anchors the label inside it", () => {
    for (const map of maps) {
      const names = map.territories.map((t) => t.name);
      expect(new Set(names).size).toBe(names.length);
      for (const territory of map.territories) {
        expect(territory.hexIds).toContain(hexId(territory.labelAnchor.q, territory.labelAnchor.r));
      }
    }
  });

  it("assigns only known terrain types and generally shows a mix", () => {
    for (const map of maps) {
      const present = new Set(map.tiles.map((t) => t.terrain));
      for (const terrain of present) expect(TERRAIN_TYPES).toContain(terrain);
      expect(present.size).toBeGreaterThanOrEqual(4);
    }
  });

  it("produces a different map for a different seed", () => {
    const hashes = new Set(maps.map(hashGeneratedMap));
    expect(hashes.size).toBe(maps.length);
  });
});

// ---------------------------------------------------------------------------
// Failure behaviour
// ---------------------------------------------------------------------------

describe("generation failure handling", () => {
  it("rejects an unsupported player count rather than guessing a profile", () => {
    expect(() => generateHexMap({ seed: "x", playerCount: 1 })).toThrow(/No map profile/);
    expect(() => generateHexMap({ seed: "x", playerCount: 5 })).toThrow(/No map profile/);
    expect(() => mapProfileFor(0)).toThrow();
  });

  it("reports seed and attempt budget when generation cannot succeed", () => {
    const error = new MapGenerationError("bad-seed", 32, ["land mask is not connected"]);
    expect(error.name).toBe("MapGenerationError");
    expect(error.seed).toBe("bad-seed");
    expect(error.attempts).toBe(32);
    expect(error.message).toContain("bad-seed");
  });

  it("detects a corrupted snapshot through the validator", () => {
    const map = generateHexMap({ seed: "corrupt", playerCount: 2 });
    const profile = mapProfileFor(2);
    expect(validateGeneratedMap(map, profile)).toEqual([]);

    const broken: GeneratedMap = {
      ...map,
      territories: map.territories.map((t, i) =>
        i === 0 ? { ...t, adjacentTerritoryIds: [] } : t,
      ),
    };
    expect(validateGeneratedMap(broken, profile).join(" ")).toContain("isolated");
  });
});

/** Deep clone with every object's keys re-inserted in the opposite order. */
function reverseKeyOrder(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reverseKeyOrder);
  if (value === null || typeof value !== "object") return value;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(value as Record<string, unknown>).toReversed()) {
    out[key] = reverseKeyOrder((value as Record<string, unknown>)[key]);
  }
  return out;
}

function isConnectedHexes(ids: readonly string[]): boolean {
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
