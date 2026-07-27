import type { GamePhase, GameStatus } from "../domain/aggregate.ts";
import type { RiskErrorCode } from "../domain/commands.ts";
import type { RiskErrorCodeV2 } from "../domain/commands-v2.ts";
import type { DecisionContext } from "./decision.ts";
import type { DecisionContextV2 } from "./decision-v2.ts";
import type { GameEvent } from "../domain/events.ts";
import type { GameEventV2 } from "../domain/events-v2.ts";
import type {
  BoardView,
  ProjectedGame,
  ProjectedMove,
  ProjectedPlayer,
  ProjectedTerritory,
  ProjectionState,
} from "../board/projection.ts";
import type {
  BoardViewV2,
  ProjectedCombatV2,
  ProjectedContinentV2,
  ProjectedGameV2,
  ProjectedHexV2,
  ProjectedMoveV2,
  ProjectedPlayerV2,
  ProjectedTerritoryV2,
  ProjectedTurnV2,
  ProjectionStateV2,
} from "../board/projection-v2.ts";
import type { GameActionV2, PlayCommandV2 } from "../domain/commands-v2.ts";

export type ApiErrorCode =
  | RiskErrorCode
  | RiskErrorCodeV2
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "WRONG_GAME"
  | "NOT_FOUND"
  | "BAD_REQUEST"
  | "PROJECTION_UNAVAILABLE"
  | "INTERNAL";

const FRIENDLY_ERRORS: Record<ApiErrorCode, string> = {
  GAME_NOT_FOUND: "That game could not be found. Check the invite link and try again.",
  GAME_ALREADY_EXISTS: "This game already exists.",
  GAME_ALREADY_STARTED: "This game has already started.",
  GAME_NOT_STARTED: "The host has not started the game yet.",
  GAME_FINISHED: "This game is already finished.",
  NOT_ENOUGH_PLAYERS: "Invite at least one more player before starting.",
  TOO_MANY_PLAYERS: "This game already has the maximum number of players.",
  PLAYER_ID_TAKEN: "That player identity is already in use.",
  COLOR_TAKEN: "That colour was just claimed. Choose another available colour and try again.",
  UNKNOWN_PLAYER: "This player is not part of the game.",
  NOT_YOUR_TURN: "It is another player’s turn.",
  STALE_TURN: "The turn changed before that move arrived. The board is now up to date.",
  INVALID_PHASE: "That move is not available in the current phase.",
  ILLEGAL_ACTION: "That move is not legal on the current board.",
  UNKNOWN_TERRITORY: "That territory does not exist.",
  NOT_ADJACENT: "Those territories are not connected.",
  INSUFFICIENT_ARMIES: "There are not enough armies for that move.",
  COMMAND_ID_REUSED: "That move identifier was already used for a different move.",
  MAP_GENERATION_FAILED: "The map could not be generated for this game.",
  PENDING_DEFENSE: "An attack is waiting for the defender to roll.",
  PENDING_OCCUPATION: "The captured country must be occupied before anything else.",
  NOT_DEFENDING_PLAYER: "Only the defending player can roll for that attack.",
  ATTACK_ID_MISMATCH: "That attack is not the one currently open.",
  ATTACK_ALREADY_RESOLVED: "That attack has already been resolved.",
  DEFENSE_DEADLINE_EXPIRED: "The defence window closed before that roll arrived.",
  INVALID_OCCUPATION: "That number of armies cannot be moved into the captured country.",
  NO_FRIENDLY_PATH: "No path of your own countries connects those territories.",
  UNAUTHORIZED: "Your player session is missing or has expired.",
  FORBIDDEN: "Your player role cannot perform that action.",
  WRONG_GAME: "This player session belongs to another game.",
  NOT_FOUND: "The requested resource could not be found.",
  BAD_REQUEST: "The request was incomplete or malformed.",
  PROJECTION_UNAVAILABLE: "The live board is temporarily unavailable.",
  INTERNAL: "Something unexpected happened. Please try again.",
};

export function friendlyError(code: ApiErrorCode, fallback?: string): string {
  return FRIENDLY_ERRORS[code] ?? fallback ?? "The move was rejected.";
}

export function statusForErrorCode(code: ApiErrorCode): number {
  switch (code) {
    case "NOT_YOUR_TURN":
    case "STALE_TURN":
    case "INVALID_PHASE":
    case "ILLEGAL_ACTION":
    case "INSUFFICIENT_ARMIES":
    case "NOT_ADJACENT":
    case "UNKNOWN_TERRITORY":
    case "GAME_FINISHED":
    case "GAME_ALREADY_STARTED":
    case "GAME_NOT_STARTED":
    case "NOT_ENOUGH_PLAYERS":
    case "TOO_MANY_PLAYERS":
    case "PLAYER_ID_TAKEN":
    case "COLOR_TAKEN":
    case "COMMAND_ID_REUSED":
    // The v2 combat interrupt: every one of these means "the canonical board
    // moved on, or is waiting on someone else" — a conflict, not a bad request.
    case "PENDING_DEFENSE":
    case "PENDING_OCCUPATION":
    case "NOT_DEFENDING_PLAYER":
    case "ATTACK_ID_MISMATCH":
    case "ATTACK_ALREADY_RESOLVED":
    case "DEFENSE_DEADLINE_EXPIRED":
    case "INVALID_OCCUPATION":
    case "NO_FRIENDLY_PATH":
      return 409;
    case "UNAUTHORIZED":
      return 401;
    case "FORBIDDEN":
    case "WRONG_GAME":
      return 403;
    case "GAME_NOT_FOUND":
    case "NOT_FOUND":
      return 404;
    default:
      return 400;
  }
}

export interface ApiErrorResponse {
  status: "rejected";
  error: { code: ApiErrorCode; message: string; currentTurnId?: string };
}

export interface PlayerIdentity {
  id: string;
  name: string;
  color: string;
  role: "host" | "player";
}

export interface CommandAck {
  status: "accepted" | "duplicate";
  commandId: string;
  sourceStreamId: string;
  sourceOffset: string;
  /** Final board-projection transition for this accepted command. */
  txid: string;
  events: Array<GameEvent | GameEventV2>;
  turnId?: string;
}

export interface CreateGameRequest {
  name?: string;
  color?: string;
  commandId?: string;
  /**
   * Ruleset for the new game. Absent means `risk-demo-v2`; pass `risk-demo-v1`
   * explicitly to create a legacy fixed-map game (design spec §11).
   */
  ruleset?: string;
  /**
   * Public seat vocabulary. `"agent"` means an external coding agent; the
   * deterministic showcase uses the explicit `"bot"` value.
   */
  controller?: "human" | "bot" | "agent";
  /** Explicit map seed, for demos and deterministic tests only. */
  mapSeed?: string;
}

export interface CreateGameResponse {
  game: { id: string; ruleset: string; mapVersion: string };
  player: PlayerIdentity;
  capability: string;
  /** Present when the host seat belongs to an external agent. */
  agentInstructions?: string;
  ack: CommandAck;
}

export interface JoinGameRequest {
  name?: string;
  color?: string;
  commandId?: string;
  controller?: "human" | "bot" | "agent";
}

export interface JoinGameResponse {
  player: PlayerIdentity;
  capability: string;
  /** Present when an external-agent seat is created; ready to paste into any fetch-capable agent. */
  agentInstructions?: string;
  ack: CommandAck;
}

export interface GameResponse {
  gameId: string;
  status: GameStatus;
  ruleset: string;
  mapVersion: string;
  round: number;
  activePlayerId?: string;
  phase?: GamePhase;
  winnerId?: string;
  generation: string;
  boardStreamId: string;
  players: Array<Pick<ProjectedPlayer, "id" | "name" | "color" | "eliminated">>;
  /** Present only for `risk-demo-v2`: the open combat interrupt, if any. */
  pendingInteraction?: DecisionContextV2["pendingInteraction"];
}

export interface BoardResponse {
  gameId: string;
  ruleset: string;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
  game: ProjectedGame;
  players: ProjectedPlayer[];
  territories: ProjectedTerritory[];
  view: BoardView;
}

/**
 * `GET /board` for a `risk-demo-v2` game. Static map rows (hexes, territories,
 * continents) are served here, once, rather than repeated on every `/decision`
 * fetch; `turn` and `combat` are the zero-or-one current-turn rows.
 */
export interface BoardResponseV2 {
  gameId: string;
  ruleset: string;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
  /** Durable State stream a browser can follow live for this generation. */
  boardStreamId: string;
  reducerVersion: string;
  game: ProjectedGameV2;
  players: ProjectedPlayerV2[];
  hexes: ProjectedHexV2[];
  territories: ProjectedTerritoryV2[];
  continents: ProjectedContinentV2[];
  turn: ProjectedTurnV2 | null;
  combat: ProjectedCombatV2 | null;
  moves: ProjectedMoveV2[];
  view: BoardViewV2;
}

export type DecisionResponse = DecisionContext;
export type DecisionResponseV2 = DecisionContextV2;

export type PlayAction =
  | { type: "reinforce"; territoryId: string; armies: number }
  | { type: "attack"; from: string; to: string; attackerDice: number }
  | { type: "fortify"; from: string; to: string; armies: number }
  | { type: "end-turn" };

export interface PlayCommandRequest {
  commandId: string;
  turnId: string;
  action: PlayAction;
}

/**
 * The v2 command envelope. Version-discriminated on purpose: v1 `attack` is a
 * whole fight-and-occupy step, while v2 `declare-attack` is one throw that opens
 * a defence interrupt. Neither is a rename of the other (design spec §11).
 */
export type PlayActionV2 = GameActionV2;
export type PlayCommandRequestV2 = PlayCommandV2;

export interface BoardProjectionMeta {
  sourceStreamId: string;
  sourceThroughOffset: string;
  sourceSeq: number;
  generation: string;
  reducerVersion: string;
  snapshot: ProjectionState;
}

export interface BoardRows {
  game: ProjectedGame;
  players: ProjectedPlayer[];
  territories: ProjectedTerritory[];
  moves: ProjectedMove[];
  meta: BoardProjectionMeta | null;
}

export interface BoardProjectionMetaV2 {
  sourceStreamId: string;
  sourceThroughOffset: string;
  sourceSeq: number;
  generation: string;
  reducerVersion: string;
  snapshot: ProjectionStateV2;
}

/** The client-side shape of one v2 board generation's collections. */
export interface BoardRowsV2 {
  game: ProjectedGameV2;
  players: ProjectedPlayerV2[];
  hexes: ProjectedHexV2[];
  territories: ProjectedTerritoryV2[];
  continents: ProjectedContinentV2[];
  turn: ProjectedTurnV2 | null;
  combat: ProjectedCombatV2 | null;
  moves: ProjectedMoveV2[];
  meta: BoardProjectionMetaV2 | null;
}
