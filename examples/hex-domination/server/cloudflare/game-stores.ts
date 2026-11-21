/**
 * {@link Stores} over one game object's SQLite database.
 *
 * The Bun host keeps these tables beside BunStorage in one SQLite file. Here they
 * sit beside DurableObjectStorage in the object's database. The table layout
 * matches `../persistence/sqlite-store.ts`. Rows are decoded with Schema, and
 * `activate` uses one `transactionSync`, as the Bun store uses one transaction.
 */
import type { DurableObjectStorage, SqlStorage } from "@cloudflare/workers-types";
import { Schema } from "effect";
import {
  parseCapabilityRole,
  parseCommandStatus,
  parseDecisionErrorJson,
  parseEventsJson,
  parseGenerationStatus,
  type GameRow,
  type GenerationRow,
  type Stores,
} from "../persistence/stores.ts";

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

const CapabilityDbRow = Schema.Struct({
  token_id: Schema.String,
  verifier_hash: Schema.String,
  game_id: Schema.String,
  player_id: Schema.String,
  role: Schema.String,
  created_at: Schema.Finite,
});

const GameDbRow = Schema.Struct({
  game_id: Schema.String,
  source_stream_id: Schema.String,
  projection_stream_id: Schema.String,
  generation: Schema.String,
  created_at: Schema.Finite,
});

const CommandDbRow = Schema.Struct({
  game_id: Schema.String,
  command_id: Schema.String,
  payload_hash: Schema.String,
  status: Schema.String,
  source_offset: Schema.NullOr(Schema.String),
  events_json: Schema.NullOr(Schema.String),
  error_json: Schema.NullOr(Schema.String),
  created_at: Schema.Finite,
});

const GenerationDbRow = Schema.Struct({
  game_id: Schema.String,
  generation: Schema.String,
  stream_id: Schema.String,
  reducer_version: Schema.String,
  status: Schema.String,
  source_through_offset: Schema.NullOr(Schema.String),
  created_at: Schema.Finite,
});

const decodeCapability = Schema.decodeUnknownSync(CapabilityDbRow);
const decodeGame = Schema.decodeUnknownSync(GameDbRow);
const decodeCommand = Schema.decodeUnknownSync(CommandDbRow);
const decodeGeneration = Schema.decodeUnknownSync(GenerationDbRow);

const gameFromDb = (row: typeof GameDbRow.Type): GameRow => ({
  gameId: row.game_id,
  sourceStreamId: row.source_stream_id,
  projectionStreamId: row.projection_stream_id,
  generation: row.generation,
  createdAt: row.created_at,
});

const generationFromDb = (row: typeof GenerationDbRow.Type): GenerationRow => ({
  gameId: row.game_id,
  generation: row.generation,
  streamId: row.stream_id,
  reducerVersion: row.reducer_version,
  status: parseGenerationStatus(row.status),
  sourceThroughOffset: row.source_through_offset,
  createdAt: row.created_at,
});

/** The first row of a query, or null. */
const first = (sql: SqlStorage, query: string, ...bindings: Array<string | number | null>) => {
  const rows = sql.exec(query, ...bindings).toArray();
  return rows[0] ?? null;
};

/** Create the metadata tables. Callers run this only for an object bound to a game. */
export function initializeGameTables(sql: SqlStorage): void {
  for (const statement of SCHEMA) sql.exec(statement);
}

/** Synchronous metadata stores over the selected game object's SQLite database. */
export function createGameStores(storage: DurableObjectStorage): Stores {
  const sql = storage.sql;
  initializeGameTables(sql);

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
        const found = first(sql, "select * from risk_capabilities where token_id = ?", tokenId);
        if (found === null) return null;
        const row = decodeCapability(found);
        return {
          tokenId: row.token_id,
          verifierHash: row.verifier_hash,
          gameId: row.game_id,
          playerId: row.player_id,
          role: parseCapabilityRole(row.role),
          createdAt: row.created_at,
        };
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
        const found = first(sql, "select * from risk_games where game_id = ?", gameId);
        return found === null ? null : gameFromDb(decodeGame(found));
      },
      list: () =>
        sql
          .exec("select * from risk_games order by created_at asc")
          .toArray()
          .map((row) => gameFromDb(decodeGame(row))),
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
        const found = first(
          sql,
          "select * from risk_commands where game_id = ? and command_id = ?",
          gameId,
          commandId,
        );
        if (found === null) return null;
        const row = decodeCommand(found);
        return {
          gameId: row.game_id,
          commandId: row.command_id,
          payloadHash: row.payload_hash,
          status: parseCommandStatus(row.status),
          sourceOffset: row.source_offset ?? undefined,
          events: row.events_json === null ? undefined : parseEventsJson(row.events_json),
          error: row.error_json === null ? undefined : parseDecisionErrorJson(row.error_json),
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
        const found = first(
          sql,
          "select * from risk_generations where game_id = ? and generation = ?",
          gameId,
          generation,
        );
        return found === null ? null : generationFromDb(decodeGeneration(found));
      },
      list(gameId) {
        return sql
          .exec("select * from risk_generations where game_id = ? order by created_at asc", gameId)
          .toArray()
          .map((row) => generationFromDb(decodeGeneration(row)));
      },
      activate(gameId, generation, sourceThroughOffset) {
        // One transaction repoints the active generation: retire the old,
        // activate the new, and move the game's pointer together.
        storage.transactionSync(() => {
          const found = first(
            sql,
            "select * from risk_generations where game_id = ? and generation = ?",
            gameId,
            generation,
          );
          if (found === null) throw new Error(`unknown generation ${gameId}/${generation}`);
          const target = decodeGeneration(found);
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
          sql.exec(
            "update risk_games set generation = ?, projection_stream_id = ? where game_id = ?",
            generation,
            target.stream_id,
            gameId,
          );
        });
      },
    },
  };
}
