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

import { Schema } from "effect";
import { PlayerControllerSchema, type PlayerController } from "./events.ts";
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

/**
 * Rename a seat in the lobby. Authorization — whether the caller may rename *this*
 * seat — is an HTTP-layer question about capabilities; the decider only enforces
 * that the seat exists, the game has not started, and the name is usable.
 */
export interface RenamePlayerCommand {
  type: "rename-player";
  commandId: string;
  playerId: string;
  name: string;
}

/** Give up a seat in the lobby. Only a `human`-controlled seat may be given up. */
export interface LeaveGameCommand {
  type: "leave-game";
  commandId: string;
  playerId: string;
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
  placements: readonly ReinforcementPlacement[];
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

/** The player-facing action payload carried by `POST /commands`. */
const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const ReinforcementPlacementSchema = Schema.Struct({
  territoryId: Schema.String,
  armies: PositiveInt,
});
export const GameAction = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("reinforce"),
    placements: Schema.Array(ReinforcementPlacementSchema),
  }),
  Schema.Struct({
    type: Schema.Literal("declare-attack"),
    from: Schema.String,
    to: Schema.String,
    attackerDice: PositiveInt,
  }),
  Schema.Struct({ type: Schema.Literal("roll-defense"), attackId: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("occupy-territory"),
    attackId: Schema.String,
    armies: PositiveInt,
  }),
  Schema.Struct({
    type: Schema.Literal("fortify"),
    from: Schema.String,
    to: Schema.String,
    armies: PositiveInt,
  }),
  Schema.Struct({ type: Schema.Literal("skip-fortifications") }),
]);
export type GameAction = typeof GameAction.Type;

export const PlayCommand = Schema.Struct({
  commandId: Schema.String,
  turnId: Schema.String,
  action: GameAction,
});
export type PlayCommand = typeof PlayCommand.Type;

/** Stable command rejection codes. */
export const RISK_ERROR_CODES = [
  "GAME_NOT_FOUND",
  "GAME_ALREADY_EXISTS",
  "GAME_ALREADY_STARTED",
  "GAME_NOT_STARTED",
  "GAME_FINISHED",
  "NOT_ENOUGH_PLAYERS",
  "TOO_MANY_PLAYERS",
  "PLAYER_ID_TAKEN",
  "UNKNOWN_PLAYER",
  "INVALID_NAME",
  "NOT_YOUR_TURN",
  "STALE_TURN",
  "INVALID_PHASE",
  "ILLEGAL_ACTION",
  "UNKNOWN_TERRITORY",
  "NOT_ADJACENT",
  "INSUFFICIENT_ARMIES",
  "COMMAND_ID_REUSED",
  "MAP_GENERATION_FAILED",
  "PENDING_DEFENSE",
  "PENDING_OCCUPATION",
  "NOT_DEFENDING_PLAYER",
  "ATTACK_ID_MISMATCH",
  "ATTACK_ALREADY_RESOLVED",
  "DEFENSE_DEADLINE_EXPIRED",
  "INVALID_OCCUPATION",
  "NO_FRIENDLY_PATH",
] as const;
export const RiskErrorCode = Schema.Literals(RISK_ERROR_CODES);
export type RiskErrorCode = typeof RiskErrorCode.Type;

export const Command = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("create-game"),
    commandId: Schema.String,
    gameId: Schema.String,
    hostPlayerId: Schema.String,
    hostName: Schema.String,
    hostColor: Schema.optionalKey(Schema.String),
    hostController: PlayerControllerSchema,
    mapSeed: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("join-game"),
    commandId: Schema.String,
    playerId: Schema.String,
    name: Schema.String,
    color: Schema.optionalKey(Schema.String),
    controller: PlayerControllerSchema,
  }),
  Schema.Struct({
    type: Schema.Literal("rename-player"),
    commandId: Schema.String,
    playerId: Schema.String,
    name: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("leave-game"),
    commandId: Schema.String,
    playerId: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("delegate-agent-seat"),
    commandId: Schema.String,
    playerId: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("start-game"), commandId: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("reinforce"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    placements: Schema.Array(ReinforcementPlacementSchema),
  }),
  Schema.Struct({
    type: Schema.Literal("declare-attack"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    from: Schema.String,
    to: Schema.String,
    attackerDice: Schema.Int,
  }),
  Schema.Struct({
    type: Schema.Literal("roll-defense"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    attackId: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("occupy-territory"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    attackId: Schema.String,
    armies: Schema.Int,
  }),
  Schema.Struct({
    type: Schema.Literal("fortify"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    from: Schema.String,
    to: Schema.String,
    armies: Schema.Int,
  }),
  Schema.Struct({
    type: Schema.Literal("skip-fortifications"),
    commandId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("resolve-defense-timeout"),
    commandId: Schema.String,
    turnId: Schema.String,
    attackId: Schema.String,
  }),
]);
export type Command = typeof Command.Type;

/** Re-exported so the command service can name the plan it injects. */
export type { GameStartPlan };
