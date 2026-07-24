/**
 * Canonical `risk-demo-v2` setup events.
 *
 * Scope note: this slice covers map generation and canonical setup only. The
 * combat half of the v2 event vocabulary — `AttackDeclared`, `AttackResolved`,
 * `TerritoryOccupied`, and the pending-interaction state machine (design spec
 * §4.4–4.5, §5.1) — lands with the two-stage combat domain slice, together with
 * the v2 aggregate that folds it. Only the events this slice actually appends are
 * defined here, so the union never contains shapes nothing reads.
 *
 * The defining property of these two events: `GameCreated` carries the seed and
 * the generator's *provenance*, while `GameStarted` carries the generator's
 * complete *output*. Replay consumes the snapshot and never invokes
 * `hex-generator-v1`, so a later generator version cannot rewrite the board of a
 * game already in progress.
 */

import type { GeneratedMap, GeneratorVersionV2, MapVersionV2, RulesetV2 } from "./map-v2.ts";
import type { InitialTerritoryV2 } from "./setup-v2.ts";

/** A seat may be played by a browser human or through the machine API. */
export type PlayerController = "human" | "agent";

export interface GameCreatedV2 {
  type: "GameCreated";
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  hostColor: string;
  hostController: PlayerController;
  ruleset: RulesetV2;
  mapVersion: MapVersionV2;
  generatorVersion: GeneratorVersionV2;
  /** Server-generated 128-bit value unless explicitly supplied for a demo/test. */
  mapSeed: string;
  commandId: string;
}

export interface PlayerJoinedV2 {
  type: "PlayerJoined";
  playerId: string;
  name: string;
  color: string;
  controller: PlayerController;
  commandId: string;
}

export interface GameStartedV2 {
  type: "GameStarted";
  /** The complete generated map. Replay reads this; it never regenerates. */
  map: GeneratedMap;
  turnOrder: string[];
  initialTerritories: InitialTerritoryV2[];
  round: 1;
  commandId: string;
}

/** The subset of the v2 event vocabulary implemented by the map/setup slice. */
export type GameSetupEventV2 = GameCreatedV2 | PlayerJoinedV2 | GameStartedV2;
