/**
 * The canonical `risk-demo-v2` event vocabulary (design spec §5.1).
 *
 * The defining property of the two setup events: `GameCreated` carries the seed
 * and the generator's *provenance*, while `GameStarted` carries the generator's
 * complete *output*. Replay consumes the snapshot and never invokes
 * `hex-generator-v1`, so a later generator version cannot rewrite the board of a
 * game already in progress.
 *
 * Combat is two-staged. `AttackDeclared` records the attacker's roll and opens a
 * defence interrupt with a canonical deadline; a later `AttackResolved` records
 * the defender's roll and the outcome, whoever produced it (the human defender,
 * their browser/HTTP controller, or the timeout resolver). The same discipline applies as to the
 * map: every die is generated once by the command service and recorded here, and
 * `declaredAt`/`defenseDeadlineAt` are injected clock readings recorded as facts.
 * Replay never rolls and never asks the current clock what should have happened.
 */

import type { GeneratedMap, GeneratorVersionV2, MapVersionV2, RulesetV2 } from "./map-v2.ts";
import type { InitialTerritoryV2 } from "./setup-v2.ts";

/**
 * Canonical controller vocabulary for newly written events.
 *
 * Historical builds wrote `"agent"` for the deterministic in-process bot. That
 * legacy value is normalized to `"bot"` when canonical history is decoded.
 * External coding agents are written explicitly as `"external-agent"`, avoiding
 * any ambiguity when old games are replayed.
 */
export type PlayerController = "human" | "bot" | "external-agent";

/** Controller spellings that may exist in persisted pre-migration events. */
export type PersistedPlayerController = PlayerController | "agent";

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

export interface PlayerControllerChangedV2 {
  type: "PlayerControllerChanged";
  playerId: string;
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

/** The subset of the v2 event vocabulary that establishes the lobby and board. */
export type GameSetupEventV2 = GameCreatedV2 | PlayerJoinedV2 | GameStartedV2;

export interface ArmiesReinforcedV2 {
  type: "ArmiesReinforced";
  turnId: string;
  playerId: string;
  territoryId: string;
  armies: number;
  commandId: string;
}

/**
 * Stage one of a throw: the attacker's dice are already rolled and recorded, and
 * the defence interrupt is open until `defenseDeadlineAt`.
 *
 * `attackId` equals the declaration's `commandId`, which is what lets an
 * out-of-turn defender — or a duplicate timer delivery — name exactly one attack
 * and never attach to a later one.
 */
export interface AttackDeclaredV2 {
  type: "AttackDeclared";
  attackId: string;
  turnId: string;
  attackerId: string;
  defenderId: string;
  from: string;
  to: string;
  attackerDice: number;
  /** Sorted descending, generated once by the command service. */
  attackerRolls: number[];
  /** The legal count `min(2, defending armies)`; the defender does not choose. */
  defenderDice: number;
  declaredAt: number;
  defenseDeadlineAt: number;
  commandId: string;
}

/** Who produced the defender's roll. Never inferred by the UI — recorded here. */
export type DefenseResolutionSource = "human" | "bot" | "agent" | "timeout";
export type PersistedDefenseResolutionSource = DefenseResolutionSource | "agent-auto";

/**
 * Stage two: the defender's dice and the outcome of the comparison.
 *
 * The attacker's rolls are repeated from `AttackDeclared` deliberately, so a
 * combat result is self-contained in the move feed. Reducers verify they match
 * the pending declaration rather than trusting the repetition.
 */
export interface AttackResolvedV2 {
  type: "AttackResolved";
  attackId: string;
  turnId: string;
  attackerId: string;
  defenderId: string;
  from: string;
  to: string;
  attackerRolls: number[];
  defenderRolls: number[];
  attackerLosses: number;
  defenderLosses: number;
  territoryCaptured: boolean;
  resolutionSource: DefenseResolutionSource;
  commandId: string;
}

/**
 * The attacker's required occupation move after a capture. Ownership changes
 * here, not in `AttackResolved`, so a projected board never claims a territory
 * before its garrison has been chosen.
 */
export interface TerritoryOccupiedV2 {
  type: "TerritoryOccupied";
  attackId: string;
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  armies: number;
  previousOwnerId: string;
  commandId: string;
}

export interface ArmiesFortifiedV2 {
  type: "ArmiesFortified";
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  armies: number;
  commandId: string;
}

export interface PlayerEliminatedV2 {
  type: "PlayerEliminated";
  playerId: string;
  byPlayerId: string;
  commandId: string;
}

export interface TurnEndedV2 {
  type: "TurnEnded";
  turnId: string;
  playerId: string;
  nextPlayerId: string;
  round: number;
  commandId: string;
}

export interface GameWonV2 {
  type: "GameWon";
  playerId: string;
  commandId: string;
}

export type GameEventV2 =
  | GameCreatedV2
  | PlayerJoinedV2
  | PlayerControllerChangedV2
  | GameStartedV2
  | ArmiesReinforcedV2
  | AttackDeclaredV2
  | AttackResolvedV2
  | TerritoryOccupiedV2
  | ArmiesFortifiedV2
  | PlayerEliminatedV2
  | TurnEndedV2
  | GameWonV2;

export type GameEventV2Type = GameEventV2["type"];

/**
 * Boundary migration for canonical v2 history written before controller names
 * distinguished deterministic bots from external coding agents.
 *
 * The migration is deliberately read-time and non-destructive: stored bytes and
 * offsets remain untouched, while every fold/projection sees the current
 * unambiguous vocabulary.
 */
export function normalizeGameEventV2(value: unknown): GameEventV2 {
  const event = value as Record<string, unknown> & {
    type?: string;
    hostController?: PersistedPlayerController;
    controller?: PersistedPlayerController;
    resolutionSource?: PersistedDefenseResolutionSource;
  };
  if (event.type === "GameCreated" && event.hostController === "agent") {
    return { ...event, hostController: "bot" } as unknown as GameEventV2;
  }
  if (event.type === "PlayerJoined" && event.controller === "agent") {
    return { ...event, controller: "bot" } as unknown as GameEventV2;
  }
  if (event.type === "AttackResolved" && event.resolutionSource === "agent-auto") {
    return { ...event, resolutionSource: "bot" } as unknown as GameEventV2;
  }
  return event as unknown as GameEventV2;
}
