/**
 * Fixed `demo-map-v1` board and `risk-demo-v1` ruleset constants.
 *
 * The map is intentionally tiny: six territories arranged as a 3x2 grid so a
 * complete game (setup -> reinforce -> attack -> fortify -> elimination) can be
 * played and asserted in a handful of moves. Keeping the domain bounded keeps
 * the implementation pressure on the Streamsy event/projection machinery rather
 * than on Risk rules.
 */

export const RULESET = "risk-demo-v1";
export const MAP_VERSION = "demo-map-v1";

export type Ruleset = typeof RULESET;
export type MapVersion = typeof MAP_VERSION;

export interface TerritoryDef {
  readonly id: string;
  readonly name: string;
  readonly adjacent: readonly string[];
}

/**
 * Undirected adjacency laid out as:
 *
 * ```text
 *   alpha —— bravo —— charlie
 *     |        |         |
 *   delta —— echo  —— foxtrot
 * ```
 */
export const TERRITORIES: readonly TerritoryDef[] = [
  { id: "alpha", name: "Alpha", adjacent: ["bravo", "delta"] },
  { id: "bravo", name: "Bravo", adjacent: ["alpha", "charlie", "echo"] },
  { id: "charlie", name: "Charlie", adjacent: ["bravo", "foxtrot"] },
  { id: "delta", name: "Delta", adjacent: ["alpha", "echo"] },
  { id: "echo", name: "Echo", adjacent: ["bravo", "delta", "foxtrot"] },
  { id: "foxtrot", name: "Foxtrot", adjacent: ["charlie", "echo"] },
];

export const TERRITORY_IDS: readonly string[] = TERRITORIES.map((t) => t.id);

const ADJACENCY: ReadonlyMap<string, ReadonlySet<string>> = new Map(
  TERRITORIES.map((t) => [t.id, new Set(t.adjacent)]),
);

/** Ruleset limits, collected so tests and later batches share one source of truth. */
export const RULES = {
  minPlayers: 2,
  maxPlayers: 4,
  initialArmiesPerTerritory: 1,
  minReinforcements: 3,
  reinforcementDivisor: 3,
  maxAttackerDice: 3,
  maxDefenderDice: 2,
  dieSides: 6,
} as const;

export function isTerritory(id: string): boolean {
  return ADJACENCY.has(id);
}

export function areAdjacent(a: string, b: string): boolean {
  return ADJACENCY.get(a)?.has(b) ?? false;
}

export function adjacentTo(id: string): readonly string[] {
  return TERRITORIES.find((t) => t.id === id)?.adjacent ?? [];
}

/** Reinforcement pool granted at the start of a turn for a given territory count. */
export function reinforcementPool(ownedTerritories: number): number {
  return Math.max(
    RULES.minReinforcements,
    Math.floor(ownedTerritories / RULES.reinforcementDivisor),
  );
}
