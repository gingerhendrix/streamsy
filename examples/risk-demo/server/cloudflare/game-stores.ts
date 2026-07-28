import type { CapabilityRole } from "../capabilities.ts";
import type { DurableObjectStorage, SqlStorageValue } from "@cloudflare/workers-types";
import type {
  CommandRow,
  GameRow,
  GenerationRow,
  GenerationStatus,
  Stores,
} from "../persistence/stores.ts";
import type { DecisionError } from "../../src/domain/decide.ts";
import type { GameEvent } from "../../src/domain/events.ts";

const SCHEMA = [
  `create table if not exists risk_capabilities (
    token_id text primary key, verifier_hash text not null, game_id text not null,
    player_id text not null, role text not null, created_at integer not null
  )`,
  `create table if not exists risk_games (
    game_id text primary key, source_stream_id text not null,
    projection_stream_id text not null, generation text not null,
    created_at integer not null
  )`,
  `create table if not exists risk_commands (
    game_id text not null, command_id text not null, payload_hash text not null,
    status text not null, source_offset text, events_json text, error_json text,
    created_at integer not null, primary key (game_id, command_id)
  )`,
  `create table if not exists risk_generations (
    game_id text not null, generation text not null, stream_id text not null,
    reducer_version text not null, status text not null, source_through_offset text,
    created_at integer not null, primary key (game_id, generation)
  )`,
];

interface CapabilityDbRow {
  [key: string]: SqlStorageValue;
  token_id: string;
  verifier_hash: string;
  game_id: string;
  player_id: string;
  role: string;
  created_at: number;
}

interface GameDbRow {
  [key: string]: SqlStorageValue;
  game_id: string;
  source_stream_id: string;
  projection_stream_id: string;
  generation: string;
  created_at: number;
}

interface CommandDbRow {
  [key: string]: SqlStorageValue;
  game_id: string;
  command_id: string;
  payload_hash: string;
  status: string;
  source_offset: string | null;
  events_json: string | null;
  error_json: string | null;
  created_at: number;
}

interface GenerationDbRow {
  [key: string]: SqlStorageValue;
  game_id: string;
  generation: string;
  stream_id: string;
  reducer_version: string;
  status: string;
  source_through_offset: string | null;
  created_at: number;
}

const gameFromDb = (row: GameDbRow): GameRow => ({
  gameId: row.game_id,
  sourceStreamId: row.source_stream_id,
  projectionStreamId: row.projection_stream_id,
  generation: row.generation,
  createdAt: row.created_at,
});

const generationFromDb = (row: GenerationDbRow): GenerationRow => ({
  gameId: row.game_id,
  generation: row.generation,
  streamId: row.stream_id,
  reducerVersion: row.reducer_version,
  status: row.status as GenerationStatus,
  sourceThroughOffset: row.source_through_offset,
  createdAt: row.created_at,
});

/** Synchronous metadata stores over the selected game DO's SQLite database. */
export function createGameStores(storage: DurableObjectStorage): Stores {
  const sql = storage.sql;
  for (const statement of SCHEMA) sql.exec(statement);

  return {
    capabilities: {
      put(row) {
        sql.exec(
          `insert or replace into risk_capabilities
           (token_id, verifier_hash, game_id, player_id, role, created_at)
           values (?, ?, ?, ?, ?, ?)`,
          row.tokenId,
          row.verifierHash,
          row.gameId,
          row.playerId,
          row.role,
          row.createdAt,
        );
      },
      getByTokenId(tokenId) {
        const row = [
          ...sql.exec<CapabilityDbRow>(
            "select * from risk_capabilities where token_id = ?",
            tokenId,
          ),
        ][0];
        return row
          ? {
              tokenId: row.token_id,
              verifierHash: row.verifier_hash,
              gameId: row.game_id,
              playerId: row.player_id,
              role: row.role as CapabilityRole,
              createdAt: row.created_at,
            }
          : null;
      },
    },
    games: {
      put(row) {
        sql.exec(
          `insert or replace into risk_games
           (game_id, source_stream_id, projection_stream_id, generation, created_at)
           values (?, ?, ?, ?, ?)`,
          row.gameId,
          row.sourceStreamId,
          row.projectionStreamId,
          row.generation,
          row.createdAt,
        );
      },
      get(gameId) {
        const row = [
          ...sql.exec<GameDbRow>("select * from risk_games where game_id = ?", gameId),
        ][0];
        return row ? gameFromDb(row) : null;
      },
      list: () =>
        [...sql.exec<GameDbRow>("select * from risk_games order by created_at asc")].map(
          gameFromDb,
        ),
    },
    commands: {
      put(row) {
        sql.exec(
          `insert or replace into risk_commands
           (game_id, command_id, payload_hash, status, source_offset, events_json, error_json, created_at)
           values (?, ?, ?, ?, ?, ?, ?, ?)`,
          row.gameId,
          row.commandId,
          row.payloadHash,
          row.status,
          row.sourceOffset ?? null,
          row.events ? JSON.stringify(row.events) : null,
          row.error ? JSON.stringify(row.error) : null,
          row.createdAt,
        );
      },
      get(gameId, commandId) {
        const row = [
          ...sql.exec<CommandDbRow>(
            "select * from risk_commands where game_id = ? and command_id = ?",
            gameId,
            commandId,
          ),
        ][0];
        if (!row) return null;
        return {
          gameId: row.game_id,
          commandId: row.command_id,
          payloadHash: row.payload_hash,
          status: row.status as CommandRow["status"],
          sourceOffset: row.source_offset ?? undefined,
          events: row.events_json ? (JSON.parse(row.events_json) as GameEvent[]) : undefined,
          error: row.error_json ? (JSON.parse(row.error_json) as DecisionError) : undefined,
          createdAt: row.created_at,
        };
      },
    },
    generations: {
      put(row) {
        sql.exec(
          `insert or replace into risk_generations
           (game_id, generation, stream_id, reducer_version, status, source_through_offset, created_at)
           values (?, ?, ?, ?, ?, ?, ?)`,
          row.gameId,
          row.generation,
          row.streamId,
          row.reducerVersion,
          row.status,
          row.sourceThroughOffset,
          row.createdAt,
        );
      },
      get(gameId, generation) {
        const row = [
          ...sql.exec<GenerationDbRow>(
            "select * from risk_generations where game_id = ? and generation = ?",
            gameId,
            generation,
          ),
        ][0];
        return row ? generationFromDb(row) : null;
      },
      list(gameId) {
        return [
          ...sql.exec<GenerationDbRow>(
            "select * from risk_generations where game_id = ? order by created_at asc",
            gameId,
          ),
        ].map(generationFromDb);
      },
      activate(gameId, generation, sourceThroughOffset) {
        storage.transactionSync(() => {
          const target = [
            ...sql.exec<Record<string, SqlStorageValue> & { count: number }>(
              "select count(*) as count from risk_generations where game_id = ? and generation = ?",
              gameId,
              generation,
            ),
          ][0];
          if (!target?.count) throw new Error(`unknown generation ${gameId}/${generation}`);
          sql.exec(
            "update risk_generations set status = 'retired' where game_id = ? and status = 'active'",
            gameId,
          );
          sql.exec(
            `update risk_generations set status = 'active', source_through_offset = ?
             where game_id = ? and generation = ?`,
            sourceThroughOffset,
            gameId,
            generation,
          );
          sql.exec("update risk_games set generation = ? where game_id = ?", generation, gameId);
        });
      },
    },
  };
}
