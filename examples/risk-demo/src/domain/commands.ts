/**
 * Command envelopes accepted by the kernel.
 *
 * Every command carries a stable `commandId` used as an idempotency key: a
 * retry with the same id returns the original outcome without re-resolving any
 * randomness. Play commands additionally carry an observed `turnId`, an
 * optimistic precondition that a delayed caller cannot accidentally act during a
 * later turn.
 *
 * In this headless kernel the acting `playerId` is passed explicitly. The
 * capability/authorization layer (Batch 3) will instead derive it from a bearer
 * token; the kernel only checks that the named player is allowed to act.
 */

export interface CreateGameCommand {
  type: "create-game";
  commandId: string;
  gameId: string;
  hostPlayerId: string;
  hostName: string;
  hostColor: string;
}

export interface JoinGameCommand {
  type: "join-game";
  commandId: string;
  playerId: string;
  name: string;
  color: string;
}

export interface StartGameCommand {
  type: "start-game";
  commandId: string;
}

export interface ReinforceCommand {
  type: "reinforce";
  commandId: string;
  turnId: string;
  playerId: string;
  territoryId: string;
  armies: number;
}

export interface AttackCommand {
  type: "attack";
  commandId: string;
  turnId: string;
  playerId: string;
  from: string;
  to: string;
  attackerDice: number;
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

export interface EndTurnCommand {
  type: "end-turn";
  commandId: string;
  turnId: string;
  playerId: string;
}

export type PlayCommand = ReinforceCommand | AttackCommand | FortifyCommand | EndTurnCommand;

export type Command = CreateGameCommand | JoinGameCommand | StartGameCommand | PlayCommand;

/** Stable, machine-readable rejection codes (mirrors `agent-play-api.md`). */
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
  | "COMMAND_ID_REUSED";
