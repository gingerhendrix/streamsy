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
import type { GameEventV2 } from "../../src/domain/events-v2.ts";
import type { DecisionError } from "../../src/domain/decide.ts";
import type { DecisionErrorV2 } from "../../src/domain/decide-v2.ts";

/** The command log stores whichever ruleset's events/rejections a game speaks. */
export type AnyGameEvent = GameEvent | GameEventV2;
export type AnyDecisionError = DecisionError | DecisionErrorV2;

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
  /**
   * Which canonical ruleset this stream speaks. Recorded here so the command
   * service can pick the right fold/decide pair without first reading the stream;
   * the authoritative copy is still the `ruleset` field in `GameCreated`.
   */
  ruleset: string;
  createdAt: number;
}

export interface CommandRow {
  gameId: string;
  commandId: string;
  payloadHash: string;
  status: "accepted" | "rejected";
  sourceOffset?: string;
  events?: AnyGameEvent[];
  error?: AnyDecisionError;
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
