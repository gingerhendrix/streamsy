/**
 * `risk-demo-v2` command envelopes and rejection codes (design spec §5.2).
 *
 * The v1 discipline carries over unchanged: a stable `commandId` is the
 * idempotency key, and every play command carries an observed `turnId` as an
 * optimistic whole-turn precondition. V2 adds two things on top.
 *
 * First, `roll-defense` is the one intentional *out-of-turn* player command. It
 * still names the current `turnId` (the turn does not change while defence is
 * pending) and additionally names an `attackId`, so a delayed defender can never
 * attach their roll to a later attack.
 *
 * Second, `resolve-defense-timeout` is internal: it is authorized by the game
 * service, not by a player capability, and is deliberately absent from
 * {@link GameActionV2} so it can never arrive through the player command
 * endpoint.
 */

import type { PlayerController } from "./events-v2.ts";
import type { GameStartPlan } from "./setup-v2.ts";

export interface CreateGameCommandV2 {
  type: "create-game";
  commandId: string;
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  hostColor: string;
  hostController: PlayerController;
  /** Server-generated unless a demo/test explicitly supplies one. */
  mapSeed: string;
}

export interface JoinGameCommandV2 {
  type: "join-game";
  commandId: string;
  playerId: string;
  name: string;
  color: string;
  controller: PlayerController;
}

export interface StartGameCommandV2 {
  type: "start-game";
  commandId: string;
}

export interface ReinforceCommandV2 {
  type: "reinforce";
  commandId: string;
  turnId: string;
  playerId: string;
  territoryId: string;
  armies: number;
}

export interface DeclareAttackCommandV2 {
  type: "declare-attack";
  commandId: string;
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  attackerDice: number;
}

/**
 * The defender authorizes a roll; they never choose the dice count. The legal
 * count was fixed at declaration time and is read from canonical state.
 *
 * There is deliberately no `resolutionSource` here. Whether a roll is recorded as
 * `human`, `bot`, or `agent` follows from the defending seat's canonical
 * {@link PlayerController}, not from anything the client sends. A client cannot
 * spoof that attribution.
 */
export interface RollDefenseCommandV2 {
  type: "roll-defense";
  commandId: string;
  turnId: string;
  playerId: string;
  attackId: string;
}

export interface OccupyTerritoryCommandV2 {
  type: "occupy-territory";
  commandId: string;
  turnId: string;
  playerId: string;
  attackId: string;
  armies: number;
}

export interface FortifyCommandV2 {
  type: "fortify";
  commandId: string;
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  armies: number;
}

export interface EndTurnCommandV2 {
  type: "end-turn";
  commandId: string;
  turnId: string;
  playerId: string;
}

/**
 * Internal deadline resolver. Submitted by the durable timeout job, never by a
 * player. It matches on both `turnId` and `attackId`, so a stale timer delivered
 * after the attack it was scheduled for has closed cannot resolve a later one.
 */
export interface ResolveDefenseTimeoutCommandV2 {
  type: "resolve-defense-timeout";
  commandId: string;
  turnId: string;
  attackId: string;
}

export type PlayCommandV2Envelope =
  | ReinforceCommandV2
  | DeclareAttackCommandV2
  | RollDefenseCommandV2
  | OccupyTerritoryCommandV2
  | FortifyCommandV2
  | EndTurnCommandV2;

export type CommandV2 =
  | CreateGameCommandV2
  | JoinGameCommandV2
  | StartGameCommandV2
  | PlayCommandV2Envelope
  | ResolveDefenseTimeoutCommandV2;

/** The player-facing action payload carried by `POST /commands`. */
export type GameActionV2 =
  | { type: "reinforce"; territoryId: string; armies: number }
  | { type: "declare-attack"; from: string; to: string; attackerDice: number }
  | { type: "roll-defense"; attackId: string }
  | { type: "occupy-territory"; attackId: string; armies: number }
  | { type: "fortify"; from: string; to: string; armies: number }
  | { type: "end-turn" };

export interface PlayCommandV2 {
  commandId: string;
  turnId: string;
  action: GameActionV2;
}

/** V2 rejection codes: the v1 set plus the two-stage-combat additions. */
export type RiskErrorCodeV2 =
  | "GAME_NOT_FOUND"
  | "GAME_ALREADY_EXISTS"
  | "GAME_ALREADY_STARTED"
  | "GAME_NOT_STARTED"
  | "GAME_FINISHED"
  | "NOT_ENOUGH_PLAYERS"
  | "TOO_MANY_PLAYERS"
  | "PLAYER_ID_TAKEN"
  | "COLOR_TAKEN"
  | "UNKNOWN_PLAYER"
  | "NOT_YOUR_TURN"
  | "STALE_TURN"
  | "INVALID_PHASE"
  | "ILLEGAL_ACTION"
  | "UNKNOWN_TERRITORY"
  | "NOT_ADJACENT"
  | "INSUFFICIENT_ARMIES"
  | "COMMAND_ID_REUSED"
  | "MAP_GENERATION_FAILED"
  | "PENDING_DEFENSE"
  | "PENDING_OCCUPATION"
  | "NOT_DEFENDING_PLAYER"
  | "ATTACK_ID_MISMATCH"
  | "ATTACK_ALREADY_RESOLVED"
  | "DEFENSE_DEADLINE_EXPIRED"
  | "INVALID_OCCUPATION"
  | "NO_FRIENDLY_PATH";

/** Re-exported so the command service can name the plan it injects. */
export type { GameStartPlan };
