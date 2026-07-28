/**
 * `risk-demo-v2` ruleset constants, procedural-map types, and pure accessors.
 *
 * Where v1 pins a compile-time six-territory board (`./map.ts`), v2 carries the
 * whole board as data: a `GeneratedMap` snapshot is produced once at game start
 * and recorded verbatim in `GameStarted`. Nothing downstream — aggregate,
 * projection, or renderer — may import the generator or a compile-time territory
 * table; they read the snapshot.
 *
 * Version identifiers are data, not implicit code versions. They are carried in
 * canonical events and select the correct domain reducer, projection reducer, and
 * renderer.
 */

import type { Axial } from "./hex.ts";
import { hexDistance, parseHexId } from "./hex.ts";
import type { Rng } from "./rng.ts";

export const RULESET_V2 = "risk-demo-v2";
export const MAP_VERSION_V2 = "procedural-hex-v1";
export const GENERATOR_VERSION_V2 = "hex-generator-v1";

export type RulesetV2 = typeof RULESET_V2;
export type MapVersionV2 = typeof MAP_VERSION_V2;
export type GeneratorVersionV2 = typeof GENERATOR_VERSION_V2;

/**
 * Terrain is **visual-only** in `risk-demo-v2`. It changes tile fill, texture, and
 * map character but never dice, movement, reinforcement, or adjacency. A future
 * ruleset that gives terrain mechanical weight must take a new ruleset identifier
 * and record its modifiers explicitly; it must not reinterpret existing v2 games.
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

export interface TerritoryDefV2 {
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
  readonly mapVersion: MapVersionV2;
  readonly generatorVersion: GeneratorVersionV2;
  readonly seed: string;
  readonly widthHint: number;
  readonly heightHint: number;
  readonly tiles: readonly HexTileDef[];
  readonly territories: readonly TerritoryDefV2[];
  readonly continents: readonly ContinentDef[];
}

/** Ruleset limits shared by the v2 domain, generator, and tests. */
export const RULES_V2 = {
  minPlayers: 2,
  maxPlayers: 4,
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
      `No map profile for ${playerCount} players; expected ${RULES_V2.minPlayers}..${RULES_V2.maxPlayers}.`,
    );
  }
  return profile;
}

/**
 * Continent bonus, computed once during generation and stored in the snapshot.
 * Kept here so tests and the generator share one definition.
 */
export function continentBonus(territoryCount: number): number {
  return Math.max(RULES_V2.minContinentBonus, Math.floor(territoryCount / 2));
}

/**
 * Base reinforcement from territory count. Continent bonuses are added on top by
 * the aggregate, which knows current ownership.
 */
export function baseReinforcement(ownedTerritories: number): number {
  return Math.max(
    RULES_V2.minReinforcements,
    Math.floor(ownedTerritories / RULES_V2.reinforcementDivisor),
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
  readonly territoryById: ReadonlyMap<string, TerritoryDefV2>;
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

export function isTerritoryV2(index: MapIndex, id: string): boolean {
  return index.territoryById.has(id);
}

export function areAdjacentV2(index: MapIndex, a: string, b: string): boolean {
  return index.territoryById.get(a)?.adjacentTerritoryIds.includes(b) ?? false;
}

export function adjacentToV2(index: MapIndex, id: string): readonly string[] {
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
