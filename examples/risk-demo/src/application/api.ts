import type { GamePhase, GameStatus } from "../domain/aggregate.ts";
import { RiskErrorCode } from "../domain/commands.ts";
import { DecisionContext, PendingInteractionSchema } from "./decision.ts";
import { PlayerControllerSchema } from "../domain/events.ts";
import { Schema } from "effect";
import type {
  BoardView,
  ProjectedCombat,
  ProjectedContinent,
  ProjectedGame,
  ProjectedHex,
  ProjectedMove,
  ProjectedPlayer,
  ProjectedTerritory,
  ProjectedTurn,
  ProjectionState,
} from "../board/projection.ts";
import type { GameAction, PlayCommand } from "../domain/commands.ts";
import {
  ProjectedCombatSchema,
  ProjectedContinentSchema,
  ProjectedGameSchema,
  ProjectedHexSchema,
  ProjectedMoveSchema,
  ProjectedPlayerSchema,
  ProjectedTerritorySchema,
  ProjectedTurnSchema,
  ProjectionStateSchema,
} from "../board/schemas.ts";

export type ApiErrorCode =
  | RiskErrorCode
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "WRONG_GAME"
  | "NOT_FOUND"
  | "BAD_REQUEST"
  | "INVALID_ACTION"
  | "AGENT_SEAT_REQUIRES_HOST"
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
  UNKNOWN_PLAYER: "This player is not part of the game.",
  INVALID_NAME: "That name cannot be used. Enter at least one visible character.",
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
  INVALID_ACTION: "The command action was incomplete or malformed.",
  AGENT_SEAT_REQUIRES_HOST: "Agent seats must be created by the human host.",
  PROJECTION_UNAVAILABLE: "The live board is temporarily unavailable.",
  INTERNAL: "Something unexpected happened. Please try again.",
};

export function friendlyError(code: ApiErrorCode, fallback?: string): string {
  return FRIENDLY_ERRORS[code] ?? fallback ?? "The move was rejected.";
}

export function isApiErrorCode(value: string): value is ApiErrorCode {
  return Object.hasOwn(FRIENDLY_ERRORS, value);
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
    case "COMMAND_ID_REUSED":
    // Combat interrupts mean the canonical board moved on or is waiting on
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
  error: {
    code: ApiErrorCode;
    message: string;
    currentTurnId?: string;
    details?: Array<{ path: string; expected: string; received: unknown }>;
  };
}

export interface PlayerIdentity {
  id: string;
  name: string;
  color: string;
  role: "host" | "player" | "agent";
}

/**
 * A command ack states that the command was recorded and at which canonical
 * offset — nothing more. Outcomes (dice, captures, phase changes) are published
 * on the player's actions stream, so the ack never carries a partial `events`
 * slice that reads like the whole result of the move.
 *
 * A browser that needs to wait for the board projection to catch up derives the
 * transaction id itself with `boardProjectionTxId(commandId)`.
 */
export interface CommandAck {
  status: "accepted" | "duplicate";
  commandId: string;
  /** Echo of the submitted turn precondition; absent on lobby commands. */
  turnId?: string;
  /** Committed final canonical offset of this command's batch. */
  eventOffset: string;
}

export interface CreateGameRequest {
  name?: string;
  /**
   * Optional colour request. The server assigns the seat's
   * colour conflict-safely: a free requested colour is honoured, and an absent
   * or taken one is replaced by the first available palette colour. The
   * response's `player.color` is the colour actually issued.
   */
  color?: string;
  commandId?: string;
  /**
   * Public seat vocabulary. `"agent"` means an external coding agent; the
   * deterministic showcase uses the explicit `"bot"` value.
   */
  controller?: "human" | "bot" | "agent";
  /** Explicit map seed, for demos and deterministic tests only. */
  mapSeed?: string;
}

export interface CreateGameResponse {
  game: { id: string; mapVersion: string };
  player: PlayerIdentity;
  capability: string;
  ack: CommandAck;
}

export interface JoinGameRequest {
  name?: string;
  /** Optional colour request; assigned conflict-safely as on `CreateGameRequest`. */
  color?: string;
  commandId?: string;
  controller?: "human" | "bot" | "agent";
}

export interface JoinGameResponse {
  player: PlayerIdentity;
  capability: string;
  ack: CommandAck;
}

/** `PATCH /players/:playerId`. The only mutable seat property. */
export interface RenamePlayerRequest {
  name: string;
  commandId?: string;
}

export interface RenamePlayerResponse {
  player: { id: string; name: string };
  ack: CommandAck;
}

/** `DELETE /players/me`. Nothing to send: the capability names the seat. */
export interface LeaveGameResponse {
  playerId: string;
  ack: CommandAck;
}

export interface AgentSeatRequest {
  name?: string;
  color?: string;
  /** Delegate an existing seat (normally the host's own seat) instead of joining a new one. */
  playerId?: string;
  commandId?: string;
}

export interface AgentSeatDescriptor {
  origin: string;
  gameId: string;
  playerId: string;
  name: string;
  color: string;
  token: string;
  urls: { map: string; actions: string; decision: string; commands: string };
}

export interface AgentSeatResponse {
  seat: AgentSeatDescriptor;
  instructions: string;
}

export interface GameResponse {
  gameId: string;
  status: GameStatus;
  mapVersion: string;
  round: number;
  activePlayerId?: string;
  phase?: GamePhase;
  winnerId?: string;
  generation: string;
  boardStreamId: string;
  players: Array<Pick<ProjectedPlayer, "id" | "name" | "color" | "eliminated">>;
  /** The open combat interrupt, if any. */
  pendingInteraction?: DecisionContext["pendingInteraction"];
}

/**
 * `GET /board`. Static map rows (hexes, territories,
 * continents) are served here, once, rather than repeated on every `/decision`
 * fetch; `turn` and `combat` are the zero-or-one current-turn rows.
 */
export interface BoardResponse {
  gameId: string;
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
  /** Durable State stream a browser can follow live for this generation. */
  boardStreamId: string;
  reducerVersion: string;
  game: ProjectedGame;
  players: ProjectedPlayer[];
  hexes: ProjectedHex[];
  territories: ProjectedTerritory[];
  continents: ProjectedContinent[];
  turn: ProjectedTurn | null;
  combat: ProjectedCombat | null;
  moves: ProjectedMove[];
  view: BoardView;
}

export type DecisionResponse = DecisionContext;
export type PlayAction = GameAction;
export type PlayCommandRequest = PlayCommand;

export interface BoardProjectionMeta {
  sourceStreamId: string;
  sourceThroughOffset: string;
  sourceSeq: number;
  generation: string;
  reducerVersion: string;
  snapshot: ProjectionState;
}

/** The client-side shape of one board generation's collections. */
export interface BoardRows {
  game: ProjectedGame;
  players: ProjectedPlayer[];
  hexes: ProjectedHex[];
  territories: ProjectedTerritory[];
  continents: ProjectedContinent[];
  turn: ProjectedTurn | null;
  combat: ProjectedCombat | null;
  moves: ProjectedMove[];
  meta: BoardProjectionMeta | null;
}

const MutableArray = <S extends Schema.Top>(schema: S) => Schema.mutable(Schema.Array(schema));
const OptionalText = Schema.optionalKey(Schema.String);
const PublicController = Schema.Literals(["human", "bot", "agent", "external-agent"]);
const ReinforcementStateSchema = Schema.Struct({
  base: Schema.Int,
  continents: MutableArray(Schema.Struct({ continentId: Schema.String, bonus: Schema.Int })),
  total: Schema.Int,
  remaining: Schema.Int,
});

export const ApiErrorCode = Schema.Union([
  RiskErrorCode,
  Schema.Literals([
    "UNAUTHORIZED",
    "FORBIDDEN",
    "WRONG_GAME",
    "NOT_FOUND",
    "BAD_REQUEST",
    "INVALID_ACTION",
    "AGENT_SEAT_REQUIRES_HOST",
    "PROJECTION_UNAVAILABLE",
    "INTERNAL",
  ]),
]);
export const ApiErrorResponse = Schema.Struct({
  status: Schema.Literal("rejected"),
  error: Schema.Struct({
    code: ApiErrorCode,
    message: Schema.String,
    currentTurnId: OptionalText,
    details: Schema.optionalKey(
      MutableArray(
        Schema.Struct({
          path: Schema.String,
          expected: Schema.String,
          received: Schema.Unknown,
        }),
      ),
    ),
  }),
});
export const PlayerIdentity = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  color: Schema.String,
  role: Schema.Literals(["host", "player", "agent"]),
});
export const CommandAck = Schema.Struct({
  status: Schema.Literals(["accepted", "duplicate"]),
  commandId: Schema.String,
  turnId: OptionalText,
  eventOffset: Schema.String,
});
export const CreateGameRequestSchema = Schema.Struct({
  name: OptionalText,
  color: OptionalText,
  commandId: OptionalText,
  controller: Schema.optionalKey(PublicController),
  mapSeed: OptionalText,
});
export const JoinGameRequestSchema = Schema.Struct({
  name: OptionalText,
  color: OptionalText,
  commandId: OptionalText,
  controller: Schema.optionalKey(PublicController),
});
export const RenamePlayerRequestSchema = Schema.Struct({
  name: Schema.String,
  commandId: OptionalText,
});
export const OptionalCommandRequestSchema = Schema.Struct({ commandId: OptionalText });
export const AgentSeatRequestSchema = Schema.Struct({
  name: OptionalText,
  color: OptionalText,
  playerId: OptionalText,
  commandId: OptionalText,
});
export const CreateGameResponse = Schema.Struct({
  game: Schema.Struct({ id: Schema.String, mapVersion: Schema.String }),
  player: PlayerIdentity,
  capability: Schema.String,
  ack: CommandAck,
});
export const JoinGameResponse = Schema.Struct({
  player: PlayerIdentity,
  capability: Schema.String,
  ack: CommandAck,
});
export const RenamePlayerResponse = Schema.Struct({
  player: Schema.Struct({ id: Schema.String, name: Schema.String }),
  ack: CommandAck,
});
export const LeaveGameResponse = Schema.Struct({ playerId: Schema.String, ack: CommandAck });
export const AgentSeatResponse = Schema.Struct({
  seat: Schema.Struct({
    origin: Schema.String,
    gameId: Schema.String,
    playerId: Schema.String,
    name: Schema.String,
    color: Schema.String,
    token: Schema.String,
    urls: Schema.Struct({
      map: Schema.String,
      actions: Schema.String,
      decision: Schema.String,
      commands: Schema.String,
    }),
  }),
  instructions: Schema.String,
});
export const GameResponse = Schema.Struct({
  gameId: Schema.String,
  status: Schema.Literals(["lobby", "playing", "finished"]),
  mapVersion: Schema.String,
  round: Schema.Int,
  activePlayerId: OptionalText,
  phase: Schema.optionalKey(Schema.Literals(["reinforce", "attack", "fortify"])),
  winnerId: OptionalText,
  generation: Schema.String,
  boardStreamId: Schema.String,
  players: MutableArray(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      color: Schema.String,
      eliminated: Schema.Boolean,
    }),
  ),
  pendingInteraction: Schema.optionalKey(PendingInteractionSchema),
});
const BoardViewSchema = Schema.Struct({
  status: Schema.Literals(["lobby", "playing", "finished"]),
  phase: Schema.optionalKey(Schema.Literals(["reinforce", "attack", "fortify"])),
  activePlayerId: OptionalText,
  turnId: OptionalText,
  round: Schema.Int,
  winnerId: OptionalText,
  players: MutableArray(
    Schema.Struct({
      id: Schema.String,
      controller: PlayerControllerSchema,
      eliminated: Schema.Boolean,
    }),
  ),
  territories: MutableArray(
    Schema.Struct({
      id: Schema.String,
      ownerId: OptionalText,
      armies: Schema.Int,
      continentId: Schema.String,
      adjacentTerritoryIds: MutableArray(Schema.String),
    }),
  ),
  continents: MutableArray(
    Schema.Struct({
      id: Schema.String,
      territoryIds: MutableArray(Schema.String),
      reinforcementBonus: Schema.Int,
      controllerId: OptionalText,
    }),
  ),
  reinforcement: ReinforcementStateSchema,
  pending: Schema.optionalKey(PendingInteractionSchema),
});
export const BoardResponse = Schema.Struct({
  gameId: Schema.String,
  sourceStreamId: Schema.String,
  sourceThroughOffset: Schema.NullOr(Schema.String),
  generation: Schema.String,
  boardStreamId: Schema.String,
  reducerVersion: Schema.String,
  game: ProjectedGameSchema,
  players: MutableArray(ProjectedPlayerSchema),
  hexes: MutableArray(ProjectedHexSchema),
  territories: MutableArray(ProjectedTerritorySchema),
  continents: MutableArray(ProjectedContinentSchema),
  turn: Schema.NullOr(ProjectedTurnSchema),
  combat: Schema.NullOr(ProjectedCombatSchema),
  moves: MutableArray(ProjectedMoveSchema),
  view: BoardViewSchema,
});
export const DecisionResponse = DecisionContext;
export const BoardProjectionMeta = Schema.Struct({
  sourceStreamId: Schema.String,
  sourceThroughOffset: Schema.String,
  sourceSeq: Schema.Int,
  generation: Schema.String,
  reducerVersion: Schema.String,
  snapshot: ProjectionStateSchema,
});

export const isApiErrorResponse = Schema.is(ApiErrorResponse);
