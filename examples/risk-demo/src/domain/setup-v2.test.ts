import { describe, expect, it } from "vitest";

import type { GameCreatedV2, GameStartedV2 } from "./events-v2.ts";
import { canonicalJson, generateHexMap, hashGeneratedMap } from "./hex-generator.ts";
import {
  GENERATOR_VERSION_V2,
  MAP_PROFILES,
  MAP_VERSION_V2,
  RULESET_V2,
  RULES_V2,
  generateMapSeed,
  indexMap,
  mapProfileFor,
} from "./map-v2.ts";
import { createSeededRng } from "./rng.ts";
import { planGameSetup, planGameStart } from "./setup-v2.ts";

const PLAYERS = ["p_ada", "p_bob", "p_cai", "p_dee"];

function playersFor(count: number): string[] {
  return PLAYERS.slice(0, count);
}

describe.each(MAP_PROFILES)("v2 setup allocation: $players players", (profile) => {
  const seeds = Array.from({ length: 25 }, (_, i) => `setup-${i}`);
  const plans = seeds.map((seed) =>
    planGameStart({ mapSeed: seed, playerIds: playersFor(profile.players) }),
  );

  it("puts every joined player into a shuffled turn order exactly once", () => {
    for (const plan of plans) {
      expect(plan.turnOrder.toSorted()).toEqual(playersFor(profile.players).toSorted());
      expect(new Set(plan.turnOrder).size).toBe(profile.players);
    }
    // The order is seeded, so it must not be the lobby order on every seed.
    const asJoined = plans.filter(
      (p) => canonicalJson(p.turnOrder) === canonicalJson(playersFor(profile.players)),
    );
    expect(asJoined.length).toBeLessThan(plans.length);
  });

  it("deals every territory exactly once, differing by at most one country", () => {
    for (const plan of plans) {
      expect(plan.initialTerritories).toHaveLength(profile.territories);
      expect(new Set(plan.initialTerritories.map((t) => t.territoryId)).size).toBe(
        profile.territories,
      );

      const counts = new Map<string, number>();
      for (const playerId of plan.turnOrder) counts.set(playerId, 0);
      for (const territory of plan.initialTerritories) {
        counts.set(territory.ownerId, counts.get(territory.ownerId)! + 1);
      }
      const values = [...counts.values()];
      expect(Math.max(...values) - Math.min(...values)).toBeLessThanOrEqual(1);
    }
  });

  it("spends each player's starting budget exactly", () => {
    for (const plan of plans) {
      const spent = new Map<string, number>();
      for (const playerId of plan.turnOrder) spent.set(playerId, 0);
      for (const territory of plan.initialTerritories) {
        spent.set(territory.ownerId, spent.get(territory.ownerId)! + territory.armies);
      }
      for (const playerId of plan.turnOrder) {
        expect(spent.get(playerId)).toBe(profile.startingArmies);
      }
    }
  });

  it("leaves at least one army on every owned country", () => {
    for (const plan of plans) {
      for (const territory of plan.initialTerritories) {
        expect(territory.armies).toBeGreaterThanOrEqual(RULES_V2.initialArmiesPerTerritory);
      }
    }
  });

  it("spreads armies evenly by always filling the lowest country", () => {
    for (const plan of plans) {
      const byOwner = new Map<string, number[]>();
      for (const territory of plan.initialTerritories) {
        const list = byOwner.get(territory.ownerId) ?? [];
        list.push(territory.armies);
        byOwner.set(territory.ownerId, list);
      }
      // Repeatedly placing on the current minimum can never leave a spread > 1.
      for (const armies of byOwner.values()) {
        expect(Math.max(...armies) - Math.min(...armies)).toBeLessThanOrEqual(1);
      }
    }
  });

  it("references only territories that exist on the recorded map", () => {
    for (const plan of plans) {
      const index = indexMap(plan.map);
      for (const territory of plan.initialTerritories) {
        expect(index.territoryById.has(territory.territoryId)).toBe(true);
      }
    }
  });

  it("emits initial territories in stable id order", () => {
    for (const plan of plans) {
      const ids = plan.initialTerritories.map((t) => t.territoryId);
      expect(ids).toEqual(ids.toSorted());
    }
  });
});

describe("v2 setup determinism", () => {
  it("produces an identical plan for the same seed and lobby", () => {
    const a = planGameStart({ mapSeed: "det-seed", playerIds: playersFor(3) });
    const b = planGameStart({ mapSeed: "det-seed", playerIds: playersFor(3) });
    expect(canonicalJson(b)).toBe(canonicalJson(a));
  });

  it("derives the allocation from the map seed, not from call order", () => {
    const map = generateHexMap({ seed: "shared-seed", playerCount: 2 });
    const first = planGameSetup({ map, playerIds: playersFor(2) });
    const second = planGameSetup({ map, playerIds: playersFor(2) });
    expect(canonicalJson(second)).toBe(canonicalJson(first));
  });

  it("diverges for a different seed", () => {
    const a = planGameStart({ mapSeed: "seed-a", playerIds: playersFor(2) });
    const b = planGameStart({ mapSeed: "seed-b", playerIds: playersFor(2) });
    expect(canonicalJson(b)).not.toBe(canonicalJson(a));
  });

  it("rejects a lobby the map profile cannot seat", () => {
    const map = generateHexMap({ seed: "mismatch", playerCount: 2 });
    expect(() => planGameSetup({ map, playerIds: playersFor(3) })).toThrow(/territories/);
    expect(() => planGameSetup({ map, playerIds: ["p_ada", "p_ada"] })).toThrow(/distinct/);
  });
});

// ---------------------------------------------------------------------------
// Canonical recording and replay independence
// ---------------------------------------------------------------------------

describe("v2 canonical recording", () => {
  it("records seed and generator provenance on GameCreated", () => {
    const mapSeed = generateMapSeed(createSeededRng(2024));
    const created: GameCreatedV2 = {
      type: "GameCreated",
      gameId: "game_1",
      hostPlayerId: "p_ada",
      hostName: "Ada",
      hostColor: "#c33",
      hostController: "human",
      ruleset: RULESET_V2,
      mapVersion: MAP_VERSION_V2,
      generatorVersion: GENERATOR_VERSION_V2,
      mapSeed,
      commandId: "cmd-create",
    };

    expect(created.ruleset).toBe("risk-demo-v2");
    expect(created.mapVersion).toBe("procedural-hex-v1");
    expect(created.generatorVersion).toBe("hex-generator-v1");
    expect(created.mapSeed).toMatch(/^[0-9a-f]{32}$/);
  });

  it("carries the whole map in GameStarted so replay needs no generator", () => {
    const mapSeed = generateMapSeed(createSeededRng(77));
    const plan = planGameStart({ mapSeed, playerIds: playersFor(4) });
    const started: GameStartedV2 = {
      type: "GameStarted",
      map: plan.map,
      turnOrder: [...plan.turnOrder],
      initialTerritories: [...plan.initialTerritories],
      round: 1,
      commandId: "cmd-start",
    };

    // A replaying consumer only ever sees the serialized event.
    const replayed = JSON.parse(JSON.stringify(started)) as GameStartedV2;
    expect(hashGeneratedMap(replayed.map)).toBe(hashGeneratedMap(plan.map));
    expect(replayed.map.tiles).toHaveLength(mapProfileFor(4).hexes);
    expect(replayed.initialTerritories).toHaveLength(mapProfileFor(4).territories);
    expect(replayed.turnOrder).toHaveLength(4);
    expect(replayed.round).toBe(1);

    // Everything a reducer needs is reachable from the snapshot alone.
    const index = indexMap(replayed.map);
    for (const territory of replayed.map.territories) {
      expect(index.continentById.has(territory.continentId)).toBe(true);
      for (const neighbour of territory.adjacentTerritoryIds) {
        expect(index.territoryById.has(neighbour)).toBe(true);
      }
    }
  });

  it("regenerates the identical board from the recorded seed for audit", () => {
    const mapSeed = generateMapSeed(createSeededRng(5));
    const plan = planGameStart({ mapSeed, playerIds: playersFor(3) });
    const audit = generateHexMap({ seed: mapSeed, playerCount: 3 });
    expect(hashGeneratedMap(audit)).toBe(hashGeneratedMap(plan.map));
    expect(plan.map.seed).toBe(mapSeed);
  });
});
