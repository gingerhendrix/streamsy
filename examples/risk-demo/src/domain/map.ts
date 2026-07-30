/**
 * Procedural-map constants, types, and pure accessors.
 *
 * The whole board is data: a `GeneratedMap` snapshot is produced once at game start
 * and recorded verbatim in `GameStarted`. Nothing downstream — aggregate,
 * projection, or renderer — may import the generator or a compile-time territory
 * table; they read the snapshot.
 *
 * Map and generator identifiers are recorded provenance for deterministic replay.
 */

import type { Axial } from "./hex.ts";
import { hexDistance, parseHexId } from "./hex.ts";
import type { Rng } from "./rng.ts";

export const MAP_VERSION = "procedural-hex-v1";
export const GENERATOR_VERSION = "hex-generator-v2";

export type MapVersion = typeof MAP_VERSION;
export type GeneratorVersion = typeof GENERATOR_VERSION;

/**
 * Terrain is visual-only. It changes tile fill, texture, and map character but
 * never dice, movement, reinforcement, or adjacency.
 */
export const TERRAIN_TYPES = ["plains", "forest", "hills", "desert", "mountains"] as const;

export type Terrain = (typeof TERRAIN_TYPES)[number];

export interface HexTileDef {
  readonly id: string;
  readonly q: number;
  readonly r: number;
  readonly territoryId: string;
  readonly terrain: Terrain;
}

export interface TerritoryDef {
  readonly id: string;
  readonly name: string;
  readonly continentId: string;
  readonly hexIds: readonly string[];
  readonly adjacentTerritoryIds: readonly string[];
  readonly labelAnchor: Axial;
}

export interface ContinentPalette {
  readonly hue: number;
  readonly pattern: string;
}

export interface ContinentDef {
  readonly id: string;
  readonly name: string;
  readonly territoryIds: readonly string[];
  readonly reinforcementBonus: number;
  readonly palette: ContinentPalette;
}

export interface GeneratedMap {
  readonly mapVersion: MapVersion;
  readonly generatorVersion: GeneratorVersion;
  readonly seed: string;
  readonly widthHint: number;
  readonly heightHint: number;
  readonly tiles: readonly HexTileDef[];
  readonly territories: readonly TerritoryDef[];
  readonly continents: readonly ContinentDef[];
}

/** Rules shared by the domain, generator, and tests. */
export const RULES = {
  minPlayers: 2,
  maxPlayers: 4,
  /**
   * Longest seat name canonical history will record. The limit lives here rather
   * than in the browser because the muster roll, the move feed, and an agent's
   * briefing all render the recorded name — a client-side cap would only be a
   * suggestion.
   */
  maxPlayerNameLength: 24,
  initialArmiesPerTerritory: 1,
  minReinforcements: 3,
  reinforcementDivisor: 3,
  maxAttackerDice: 3,
  maxDefenderDice: 2,
  dieSides: 6,
  minTerritoryHexes: 3,
  maxTerritoryHexes: 6,
  minContinentTerritories: 3,
  minContinentBonus: 2,
  /**
   * How far above the even share a continent's target size may be pushed during
   * generation. Continents that all hold the same number of territories all pay
   * the same bonus, which makes holding one an arbitrary choice; a bounded skew
   * gives every map a continent worth fighting for and one worth trading away.
   */
  continentSizeSpread: 2,
  /** Human defence interrupt window, in milliseconds. */
  defenseTimeoutMs: 15_000,
  /** Bounded retry budget for map generation before game start is rejected. */
  maxGenerationAttempts: 32,
} as const;

/** Per-player-count map size profile. */
export interface MapProfile {
  readonly players: number;
  readonly hexes: number;
  readonly territories: number;
  readonly continents: number;
  /** Total starting army budget per player, *including* the one-per-territory seed. */
  readonly startingArmies: number;
}

export const MAP_PROFILES: readonly MapProfile[] = [
  { players: 2, hexes: 72, territories: 16, continents: 4, startingArmies: 20 },
  { players: 3, hexes: 84, territories: 18, continents: 4, startingArmies: 15 },
  { players: 4, hexes: 96, territories: 20, continents: 4, startingArmies: 12 },
];

export function mapProfileFor(playerCount: number): MapProfile {
  const profile = MAP_PROFILES.find((p) => p.players === playerCount);
  if (!profile) {
    throw new Error(
      `No map profile for ${playerCount} players; expected ${RULES.minPlayers}..${RULES.maxPlayers}.`,
    );
  }
  return profile;
}

/**
 * Largest continent a valid map may carry: the even share plus the allowed skew.
 *
 * The floor (`minContinentTerritories`) keeps a continent worth owning; this is the
 * matching ceiling. Without it a stranded pocket of countries can all fall to the
 * one continent they touch, and because the bonus rises with territory count, that
 * continent alone would decide the game. A candidate over the ceiling is rejected
 * and regenerated rather than shipped.
 */
export function maxContinentTerritories(profile: MapProfile): number {
  return Math.floor(profile.territories / profile.continents) + RULES.continentSizeSpread;
}

/**
 * Continent bonus, computed once during generation and stored in the snapshot.
 * Kept here so tests and the generator share one definition.
 *
 * One territory below the count, floored at `minContinentBonus`: strictly
 * increasing across the sizes a real map produces (3→2, 4→3, 5→4, 6→5), so a
 * larger continent is visibly worth more than a smaller one. A halved count is
 * not — at 3–6 territories it collapses to 2 or 3 and the mechanic reads as flat.
 *
 * Bonuses are frozen into the `GameStarted` map snapshot, so a change here only
 * affects games started afterwards; a game in progress keeps its recorded bonuses.
 */
export function continentBonus(territoryCount: number): number {
  return Math.max(RULES.minContinentBonus, territoryCount - 1);
}

/**
 * Base reinforcement from territory count. Continent bonuses are added on top by
 * the aggregate, which knows current ownership.
 */
export function baseReinforcement(ownedTerritories: number): number {
  return Math.max(
    RULES.minReinforcements,
    Math.floor(ownedTerritories / RULES.reinforcementDivisor),
  );
}

/**
 * Draw a 128-bit map seed as 32 lowercase hex characters from the injected Rng.
 *
 * Production creates the seed server-side at `create-game`; test and demo tooling
 * may supply one explicitly. Either way the seed is recorded in `GameCreated`
 * before any map exists, so generation is reproducible from canonical history.
 */
export function generateMapSeed(rng: Rng): string {
  let seed = "";
  for (let i = 0; i < 32; i += 1) seed += rng.nextInt(16).toString(16);
  return seed;
}

// ---------------------------------------------------------------------------
// Pure accessors over a recorded snapshot
// ---------------------------------------------------------------------------

/** Indexed view of a `GeneratedMap`, built on demand; the snapshot stays canonical. */
export interface MapIndex {
  readonly map: GeneratedMap;
  readonly territoryById: ReadonlyMap<string, TerritoryDef>;
  readonly continentById: ReadonlyMap<string, ContinentDef>;
  readonly tileById: ReadonlyMap<string, HexTileDef>;
  readonly territoryIds: readonly string[];
}

export function indexMap(map: GeneratedMap): MapIndex {
  return {
    map,
    territoryById: new Map(map.territories.map((t) => [t.id, t])),
    continentById: new Map(map.continents.map((c) => [c.id, c])),
    tileById: new Map(map.tiles.map((t) => [t.id, t])),
    territoryIds: map.territories.map((t) => t.id),
  };
}

export function isTerritory(index: MapIndex, id: string): boolean {
  return index.territoryById.has(id);
}

export function areAdjacent(index: MapIndex, a: string, b: string): boolean {
  return index.territoryById.get(a)?.adjacentTerritoryIds.includes(b) ?? false;
}

export function adjacentTo(index: MapIndex, id: string): readonly string[] {
  return index.territoryById.get(id)?.adjacentTerritoryIds ?? [];
}

/** Terrain mix of a territory, as counts keyed by terrain type. Presentation only. */
export function territoryTerrainMix(
  index: MapIndex,
  territoryId: string,
): Partial<Record<Terrain, number>> {
  const territory = index.territoryById.get(territoryId);
  if (!territory) return {};
  const mix: Partial<Record<Terrain, number>> = {};
  for (const hex of territory.hexIds) {
    const tile = index.tileById.get(hex);
    if (!tile) continue;
    mix[tile.terrain] = (mix[tile.terrain] ?? 0) + 1;
  }
  return mix;
}

/** Hex-distance between two territories' label anchors. Presentation helper. */
export function labelAnchorDistance(index: MapIndex, a: string, b: string): number {
  const from = index.territoryById.get(a)?.labelAnchor;
  const to = index.territoryById.get(b)?.labelAnchor;
  if (!from || !to) return Number.POSITIVE_INFINITY;
  return hexDistance(from, to);
}

/** Axial coordinate of a tile id, read from the snapshot rather than reparsed. */
export function tileCoordinate(index: MapIndex, hexIdValue: string): Axial {
  const tile = index.tileById.get(hexIdValue);
  return tile ? { q: tile.q, r: tile.r } : parseHexId(hexIdValue);
}
