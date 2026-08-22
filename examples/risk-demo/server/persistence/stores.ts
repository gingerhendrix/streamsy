/**
 * Durable metadata stores: capability verifiers, game records, and the command
 * idempotency/recovery log. The API is storage-agnostic behind these interfaces
 * so the app runs on either in-memory maps (tests) or SQLite (`sqlite-store.ts`).
 *
 * Only strong verifier hashes and capability metadata are ever stored — never a
 * raw token. Canonical game events and the board projection live in Streamsy
 * streams, not here.
 */

import type { CapabilityRole } from "../capabilities.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { DecisionError } from "../../src/domain/decide.ts";
import type { RiskErrorCode } from "../../src/domain/commands.ts";

const CAPABILITY_ROLES = new Set(["host", "player", "agent"]);
const GENERATION_STATUSES = new Set(["building", "active", "retired", "failed"]);
const COMMAND_STATUSES = new Set(["accepted", "rejected"]);
const EVENT_TYPES = new Set([
  "GameCreated",
  "PlayerJoined",
  "PlayerControllerChanged",
  "PlayerRenamed",
  "PlayerLeft",
  "GameStarted",
  "ArmiesReinforced",
  "AttackDeclared",
  "AttackResolved",
  "TerritoryOccupied",
  "ArmiesFortified",
  "PlayerEliminated",
  "TurnEnded",
  "GameWon",
]);
const RISK_ERROR_CODES = new Set([
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
]);

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return Object.fromEntries(Object.entries(value));
}

export function parseCapabilityRole(value: string): CapabilityRole {
  if (!CAPABILITY_ROLES.has(value)) throw new Error(`invalid persisted capability role: ${value}`);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Membership in the closed capability-role set is checked immediately above.
  return value as CapabilityRole;
}

export function parseGenerationStatus(value: string): GenerationStatus {
  if (!GENERATION_STATUSES.has(value))
    throw new Error(`invalid persisted generation status: ${value}`);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Membership in the closed generation-status set is checked immediately above.
  return value as GenerationStatus;
}

export function parseCommandStatus(value: string): CommandRow["status"] {
  if (!COMMAND_STATUSES.has(value)) throw new Error(`invalid persisted command status: ${value}`);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Membership in the closed command-status set is checked immediately above.
  return value as CommandRow["status"];
}

export function parseEventsJson(json: string): GameEvent[] {
  const value: unknown = JSON.parse(json);
  if (!Array.isArray(value)) throw new Error("persisted command events must be an array");
  for (const item of value) {
    const event = record(item, "persisted command event");
    if (
      typeof event.type !== "string" ||
      !EVENT_TYPES.has(event.type) ||
      typeof event.commandId !== "string"
    ) {
      throw new Error("persisted command event has an invalid type or commandId");
    }
  }
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Every element passed the closed event-type and command-id boundary checks above; reducers retain event-specific invariant checks.
  return value as GameEvent[];
}

export function parseDecisionErrorJson(json: string): DecisionError {
  const value = record(JSON.parse(json), "persisted decision error");
  if (
    typeof value.code !== "string" ||
    !RISK_ERROR_CODES.has(value.code) ||
    typeof value.message !== "string"
  ) {
    throw new Error("persisted decision error has an invalid code or message");
  }
  const optional = (key: "currentTurnId" | "attackId"): string | undefined => {
    const field = value[key];
    if (field !== undefined && typeof field !== "string")
      throw new Error(`persisted decision error ${key} must be a string`);
    return field;
  };
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Membership in the closed RiskErrorCode set is checked above.
  const code = value.code as RiskErrorCode;
  return {
    code,
    message: value.message,
    currentTurnId: optional("currentTurnId"),
    attackId: optional("attackId"),
  };
}

export interface CapabilityRow {
  tokenId: string;
  verifierHash: string;
  gameId: string;
  playerId: string;
  role: CapabilityRole;
  createdAt: number;
}

export interface GameRow {
  gameId: string;
  sourceStreamId: string;
  projectionStreamId: string;
  generation: string;
  createdAt: number;
}

export interface CommandRow {
  gameId: string;
  commandId: string;
  payloadHash: string;
  status: "accepted" | "rejected";
  sourceOffset?: string;
  events?: GameEvent[];
  error?: DecisionError;
  createdAt: number;
}

export type GenerationStatus = "building" | "active" | "retired" | "failed";

/** One board-projection generation for a game (the active one is retained + others too). */
export interface GenerationRow {
  gameId: string;
  generation: string;
  streamId: string;
  reducerVersion: string;
  status: GenerationStatus;
  /** Canonical watermark the generation was verified through, or null while building. */
  sourceThroughOffset: string | null;
  createdAt: number;
}

export interface CapabilityStore {
  put(row: CapabilityRow): void;
  getByTokenId(tokenId: string): CapabilityRow | null;
}

export interface GameStore {
  put(row: GameRow): void;
  get(gameId: string): GameRow | null;
  /** Every known game, oldest first. Used by restart recovery to rebuild timers. */
  list(): GameRow[];
}

export interface CommandStore {
  put(row: CommandRow): void;
  get(gameId: string, commandId: string): CommandRow | null;
}

export interface GenerationStore {
  put(row: GenerationRow): void;
  get(gameId: string, generation: string): GenerationRow | null;
  list(gameId: string): GenerationRow[];
  /**
   * Atomically make `generation` the active board projection for `gameId`:
   * mark it `active` with its verified `sourceThroughOffset`, retire any
   * previously-active generation, and repoint the game row's active-generation
   * pointer — all in one transaction. Old generations' streams are retained,
   * never deleted, so a cutover is reversible.
   */
  activate(
    gameId: string,
    generation: string,
    sourceThroughOffset: string | null,
    now: number,
  ): void;
}

export interface Stores {
  capabilities: CapabilityStore;
  games: GameStore;
  commands: CommandStore;
  generations: GenerationStore;
}

function commandKey(gameId: string, commandId: string): string {
  return `${gameId} ${commandId}`;
}

export function createInMemoryStores(): Stores {
  const capabilities = new Map<string, CapabilityRow>();
  const games = new Map<string, GameRow>();
  const commands = new Map<string, CommandRow>();
  const generations = new Map<string, GenerationRow>();

  return {
    capabilities: {
      put: (row) => void capabilities.set(row.tokenId, row),
      getByTokenId: (tokenId) => capabilities.get(tokenId) ?? null,
    },
    games: {
      put: (row) => void games.set(row.gameId, row),
      get: (gameId) => games.get(gameId) ?? null,
      list: () => [...games.values()].toSorted((a, b) => a.createdAt - b.createdAt),
    },
    commands: {
      put: (row) => void commands.set(commandKey(row.gameId, row.commandId), row),
      get: (gameId, commandId) => commands.get(commandKey(gameId, commandId)) ?? null,
    },
    generations: {
      put: (row) => void generations.set(commandKey(row.gameId, row.generation), { ...row }),
      get: (gameId, generation) => generations.get(commandKey(gameId, generation)) ?? null,
      list: (gameId) =>
        [...generations.values()]
          .filter((r) => r.gameId === gameId)
          .toSorted((a, b) => a.createdAt - b.createdAt),
      activate: (gameId, generation, sourceThroughOffset, now) => {
        const target = generations.get(commandKey(gameId, generation));
        if (!target) throw new Error(`unknown generation ${gameId}/${generation}`);
        for (const row of generations.values()) {
          if (row.gameId === gameId && row.status === "active") row.status = "retired";
        }
        target.status = "active";
        target.sourceThroughOffset = sourceThroughOffset;
        target.createdAt = target.createdAt || now;
        const game = games.get(gameId);
        if (game) games.set(gameId, { ...game, generation });
      },
    },
  };
}
