/**
 * Fast deterministic setup and allocation for `risk-demo-v2`.
 *
 * V2 deliberately has no manual claim phase. `GameStarted` atomically establishes
 * a fair, replayable initial board: turn order, a round-robin territory deal, and
 * a full army allocation. Every value here is a canonical fact recorded in the
 * event — setup randomness is consumed exactly once, by the start-game command
 * service, and replay never recomputes it.
 */

import { generateHexMap } from "./hex-generator.ts";
import { createSubstream } from "./generator-rng.ts";
import type { GeneratedMap } from "./map-v2.ts";
import { RULES_V2, mapProfileFor } from "./map-v2.ts";

export interface InitialTerritoryV2 {
  readonly territoryId: string;
  readonly ownerId: string;
  readonly armies: number;
}

export interface SetupAllocation {
  readonly turnOrder: readonly string[];
  readonly initialTerritories: readonly InitialTerritoryV2[];
}

export interface PlanSetupRequest {
  readonly map: GeneratedMap;
  /** Joined players, in lobby order. Turn order is shuffled from this list. */
  readonly playerIds: readonly string[];
}

/**
 * Deal the board and allocate armies deterministically from the map seed.
 *
 * Ownership counts differ by at most one. Each player then spends their whole
 * starting budget: one army per owned territory, with the remainder placed one at
 * a time onto their currently lowest-army territory. Ties are broken by a seeded
 * per-player territory order rather than by id, so the allocation is not biased
 * toward alphabetically-early countries.
 */
export function planGameSetup(request: PlanSetupRequest): SetupAllocation {
  const { map, playerIds } = request;
  const profile = mapProfileFor(playerIds.length);

  if (new Set(playerIds).size !== playerIds.length) {
    throw new Error("planGameSetup requires distinct player ids");
  }
  if (map.territories.length !== profile.territories) {
    throw new Error(
      `map has ${map.territories.length} territories, expected ${profile.territories} for ${playerIds.length} players`,
    );
  }

  const setup = createSubstream(map.seed, "setup");
  const turnOrder = setup.shuffle(playerIds);
  const dealt = setup.shuffle(map.territories.map((t) => t.id));

  const armies = new Map<string, number>();
  const ownerOf = new Map<string, string>();
  const owned = new Map<string, string[]>();
  for (const playerId of turnOrder) owned.set(playerId, []);

  dealt.forEach((territoryId, index) => {
    const ownerId = turnOrder[index % turnOrder.length]!;
    ownerOf.set(territoryId, ownerId);
    armies.set(territoryId, RULES_V2.initialArmiesPerTerritory);
    owned.get(ownerId)!.push(territoryId);
  });

  for (const playerId of turnOrder) {
    const held = owned.get(playerId)!;
    if (held.length === 0) {
      throw new Error(`${playerId} was dealt no territories`);
    }
    // Seeded tie-break order: earlier entries win ties for "lowest armies".
    const tieOrder = setup.shuffle(held);
    const remaining = profile.startingArmies - held.length * RULES_V2.initialArmiesPerTerritory;
    if (remaining < 0) {
      throw new Error(
        `${playerId} holds ${held.length} territories but the budget is only ${profile.startingArmies}`,
      );
    }
    for (let placed = 0; placed < remaining; placed += 1) {
      let target = tieOrder[0]!;
      for (const territoryId of tieOrder) {
        if (armies.get(territoryId)! < armies.get(target)!) target = territoryId;
      }
      armies.set(target, armies.get(target)! + 1);
    }
  }

  const initialTerritories: InitialTerritoryV2[] = map.territories
    .map((territory) => ({
      territoryId: territory.id,
      ownerId: ownerOf.get(territory.id)!,
      armies: armies.get(territory.id)!,
    }))
    .toSorted((a, b) =>
      a.territoryId < b.territoryId ? -1 : a.territoryId > b.territoryId ? 1 : 0,
    );

  return { turnOrder, initialTerritories };
}

/** The complete canonical payload of a v2 `GameStarted`. */
export interface GameStartPlan extends SetupAllocation {
  readonly map: GeneratedMap;
}

/**
 * Build the whole `GameStarted` payload from the recorded seed and the lobby.
 *
 * The start-game command service calls this exactly once, *after* `commandId`
 * deduplication and *before* the canonical append, so randomness is consumed only
 * on the path that actually commits. If the append then loses its source-head CAS,
 * the service refolds canonical history and rejects the command as already
 * started — it must not regenerate, because a second call here would produce a
 * different board than the one the winning append recorded.
 */
export function planGameStart(request: {
  readonly mapSeed: string;
  readonly playerIds: readonly string[];
}): GameStartPlan {
  const map = generateHexMap({
    seed: request.mapSeed,
    playerCount: request.playerIds.length,
  });
  const allocation = planGameSetup({ map, playerIds: request.playerIds });
  return { map, ...allocation };
}
