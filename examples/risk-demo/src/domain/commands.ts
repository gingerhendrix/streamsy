/**
 * `Hex Domination` command envelopes and rejection codes.
 *
 * A stable `commandId` is the
 * idempotency key, and every play command carries an observed `turnId` as an
 * optimistic whole-turn precondition.  adds two things on top.
 *
 * First, `roll-defense` is the one intentional *out-of-turn* player command. It
 * still names the current `turnId` (the turn does not change while defence is
 * pending) and additionally names an `attackId`, so a delayed defender can never
 * attach their roll to a later attack.
 *
 * Second, `resolve-defense-timeout` is internal: it is authorized by the game
 * service, not by a player capability, and is deliberately absent from
 * {@link GameAction} so it can never arrive through the player command
 * endpoint.
 */

import type { PlayerController } from "./events.ts";
import type { GameStartPlan } from "./setup.ts";

export interface CreateGameCommand {
  type: "create-game";
  commandId: string;
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  /** Requested colour; the decider assigns a free palette colour when absent or taken. */
  hostColor?: string;
  hostController: PlayerController;
  /** Server-generated unless a demo/test explicitly supplies one. */
  mapSeed: string;
}

export interface JoinGameCommand {
  type: "join-game";
  commandId: string;
  playerId: string;
  name: string;
  /** Requested colour; the decider assigns a free palette colour when absent or taken. */
  color?: string;
  controller: PlayerController;
}

export interface StartGameCommand {
  type: "start-game";
  commandId: string;
}

export interface DelegateAgentSeatCommand {
  type: "delegate-agent-seat";
  commandId: string;
  playerId: string;
}

export interface ReinforcementPlacement {
  territoryId: string;
  armies: number;
}

export interface ReinforceCommand {
  type: "reinforce";
  commandId: string;
  turnId: string;
  playerId: string;
  /** The complete turn allocation, committed atomically. */
  placements: ReinforcementPlacement[];
}

export interface DeclareAttackCommand {
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
export interface RollDefenseCommand {
  type: "roll-defense";
  commandId: string;
  turnId: string;
  playerId: string;
  attackId: string;
}

export interface OccupyTerritoryCommand {
  type: "occupy-territory";
  commandId: string;
  turnId: string;
  playerId: string;
  attackId: string;
  armies: number;
}

export interface FortifyCommand {
  type: "fortify";
  commandId: string;
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  armies: number;
}

export interface SkipFortificationsCommand {
  type: "skip-fortifications";
  commandId: string;
  turnId: string;
  playerId: string;
}

/**
 * Internal deadline resolver. Submitted by the durable timeout job, never by a
 * player. It matches on both `turnId` and `attackId`, so a stale timer delivered
 * after the attack it was scheduled for has closed cannot resolve a later one.
 */
export interface ResolveDefenseTimeoutCommand {
  type: "resolve-defense-timeout";
  commandId: string;
  turnId: string;
  attackId: string;
}

export type PlayCommandEnvelope =
  | ReinforceCommand
  | DeclareAttackCommand
  | RollDefenseCommand
  | OccupyTerritoryCommand
  | FortifyCommand
  | SkipFortificationsCommand;

export type Command =
  | CreateGameCommand
  | JoinGameCommand
  | DelegateAgentSeatCommand
  | StartGameCommand
  | PlayCommandEnvelope
  | ResolveDefenseTimeoutCommand;

/** The player-facing action payload carried by `POST /commands`. */
export type GameAction =
  | { type: "reinforce"; placements: ReinforcementPlacement[] }
  | { type: "declare-attack"; from: string; to: string; attackerDice: number }
  | { type: "roll-defense"; attackId: string }
  | { type: "occupy-territory"; attackId: string; armies: number }
  | { type: "fortify"; from: string; to: string; armies: number }
  | { type: "skip-fortifications" };

export interface PlayCommand {
  commandId: string;
  turnId: string;
  action: GameAction;
}

/** Stable command rejection codes. */
export type RiskErrorCode =
  | "GAME_NOT_FOUND"
  | "GAME_ALREADY_EXISTS"
  | "GAME_ALREADY_STARTED"
  | "GAME_NOT_STARTED"
  | "GAME_FINISHED"
  | "NOT_ENOUGH_PLAYERS"
  | "TOO_MANY_PLAYERS"
  | "PLAYER_ID_TAKEN"
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
