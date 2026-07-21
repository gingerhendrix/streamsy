/**
 * Durable metadata stores: capability verifiers, game records, and the command
 * idempotency/recovery log. The API is storage-agnostic behind these interfaces
 * so the app runs on either in-memory maps (tests) or SQLite (`sqlite-store.ts`).
 *
 * Only strong verifier hashes and capability metadata are ever stored — never a
 * raw token. Canonical game events and the board projection live in Streamsy
 * streams, not here.
 */

import type { CapabilityRole } from "./capabilities.ts";
import type { GameEvent } from "../src/events.ts";
import type { DecisionError } from "../src/decide.ts";

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

export interface CapabilityStore {
  put(row: CapabilityRow): void;
  getByTokenId(tokenId: string): CapabilityRow | null;
}

export interface GameStore {
  put(row: GameRow): void;
  get(gameId: string): GameRow | null;
}

export interface CommandStore {
  put(row: CommandRow): void;
  get(gameId: string, commandId: string): CommandRow | null;
}

export interface Stores {
  capabilities: CapabilityStore;
  games: GameStore;
  commands: CommandStore;
}

function commandKey(gameId: string, commandId: string): string {
  return `${gameId} ${commandId}`;
}

export function createInMemoryStores(): Stores {
  const capabilities = new Map<string, CapabilityRow>();
  const games = new Map<string, GameRow>();
  const commands = new Map<string, CommandRow>();

  return {
    capabilities: {
      put: (row) => void capabilities.set(row.tokenId, row),
      getByTokenId: (tokenId) => capabilities.get(tokenId) ?? null,
    },
    games: {
      put: (row) => void games.set(row.gameId, row),
      get: (gameId) => games.get(gameId) ?? null,
    },
    commands: {
      put: (row) => void commands.set(commandKey(row.gameId, row.commandId), row),
      get: (gameId, commandId) => commands.get(commandKey(gameId, commandId)) ?? null,
    },
  };
}
