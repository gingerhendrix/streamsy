/**
 * Canonical domain events — the source of truth for a game.
 *
 * These are the immutable facts the command service appends after validating a
 * command against a fold of prior history. The aggregate fold and the board
 * projection are both derived purely from an ordered list of these events.
 *
 * Notes on deviations from the design doc, kept deliberate for a headless
 * kernel:
 *  - `occurredAt` timestamps are omitted. Nothing in the folded state depends on
 *    wall-clock time, and omitting it keeps decision logic pure without a clock
 *    injection. The command/persistence layer owns timestamps and the
 *    stream offset; here offsets are the positional index of an event.
 *  - Dice and any other randomness are resolved once by the command service and
 *    recorded here as facts. Replay never rolls.
 */

import type { MapVersion, Ruleset } from "./map.ts";

export type GameEventType = GameEvent["type"];

export interface GameCreated {
  type: "GameCreated";
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  hostColor: string;
  ruleset: Ruleset;
  mapVersion: MapVersion;
  commandId: string;
}

export interface PlayerJoined {
  type: "PlayerJoined";
  playerId: string;
  name: string;
  color: string;
  commandId: string;
}

export interface InitialTerritory {
  territoryId: string;
  ownerId: string;
  armies: number;
}

export interface GameStarted {
  type: "GameStarted";
  turnOrder: string[];
  initialTerritories: InitialTerritory[];
  round: number;
  commandId: string;
}

export interface ArmiesReinforced {
  type: "ArmiesReinforced";
  playerId: string;
  territoryId: string;
  armies: number;
  commandId: string;
}

export interface AttackResolved {
  type: "AttackResolved";
  playerId: string;
  from: string;
  to: string;
  attackerRolls: number[];
  defenderRolls: number[];
  attackerLosses: number;
  defenderLosses: number;
  territoryCaptured: boolean;
  /** Armies moved into the captured territory; present only when captured. */
  occupyingArmies?: number;
  commandId: string;
}

export interface ArmiesFortified {
  type: "ArmiesFortified";
  playerId: string;
  from: string;
  to: string;
  armies: number;
  commandId: string;
}

export interface TurnEnded {
  type: "TurnEnded";
  playerId: string;
  nextPlayerId: string;
  round: number;
  commandId: string;
}

export interface PlayerEliminated {
  type: "PlayerEliminated";
  playerId: string;
  byPlayerId: string;
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
  | GameStarted
  | ArmiesReinforced
  | AttackResolved
  | ArmiesFortified
  | TurnEnded
  | PlayerEliminated
  | GameWon;
