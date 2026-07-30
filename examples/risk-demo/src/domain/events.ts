/**
 * The canonical Hex Domination event vocabulary.
 *
 * The defining property of the two setup events: `GameCreated` carries the seed
 * and the generator's *provenance*, while `GameStarted` carries the generator's
 * complete *output*. Replay consumes the snapshot and never invokes
 * `hex-generator-v2`, so a later generator version cannot rewrite the board of a
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

import type { GeneratedMap, GeneratorVersion, MapVersion } from "./map.ts";
import type { InitialTerritory } from "./setup.ts";

/** Canonical controller vocabulary recorded in game events. */
export type PlayerController = "human" | "bot" | "external-agent";

export interface GameCreated {
  type: "GameCreated";
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  hostColor: string;
  hostController: PlayerController;
  mapVersion: MapVersion;
  generatorVersion: GeneratorVersion;
  /** Server-generated 128-bit value unless explicitly supplied for a demo/test. */
  mapSeed: string;
  commandId: string;
}

export interface PlayerJoined {
  type: "PlayerJoined";
  playerId: string;
  name: string;
  color: string;
  controller: PlayerController;
  commandId: string;
}

export interface PlayerControllerChanged {
  type: "PlayerControllerChanged";
  playerId: string;
  controller: PlayerController;
  commandId: string;
}

export interface GameStarted {
  type: "GameStarted";
  /** The complete generated map. Replay reads this; it never regenerates. */
  map: GeneratedMap;
  turnOrder: string[];
  initialTerritories: InitialTerritory[];
  round: 1;
  commandId: string;
}

/** The subset of the event vocabulary that establishes the lobby and board. */
export type GameSetupEvent = GameCreated | PlayerJoined | GameStarted;

export interface ArmiesReinforced {
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
export interface AttackDeclared {
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

/**
 * Stage two: the defender's dice and the outcome of the comparison.
 *
 * The attacker's rolls are repeated from `AttackDeclared` deliberately, so a
 * combat result is self-contained in the move feed. Reducers verify they match
 * the pending declaration rather than trusting the repetition.
 */
export interface AttackResolved {
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
export interface TerritoryOccupied {
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

export interface ArmiesFortified {
  type: "ArmiesFortified";
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  armies: number;
  commandId: string;
}

export interface PlayerEliminated {
  type: "PlayerEliminated";
  playerId: string;
  byPlayerId: string;
  commandId: string;
}

export interface TurnEnded {
  type: "TurnEnded";
  turnId: string;
  playerId: string;
  nextPlayerId: string;
  round: number;
  commandId: string;
}

export interface GameWon {
  type: "GameWon";
  playerId: string;
  commandId: string;
}

export type GameEvent =
  | GameCreated
  | PlayerJoined
  | PlayerControllerChanged
  | GameStarted
  | ArmiesReinforced
  | AttackDeclared
  | AttackResolved
  | TerritoryOccupied
  | ArmiesFortified
  | PlayerEliminated
  | TurnEnded
  | GameWon;

export type GameEventType = GameEvent["type"];
