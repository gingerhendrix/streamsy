/**
 * SQLite-backed {@link Stores} (capability verifiers, game records, command log).
 *
 * Runs in the Bun runtime against the SAME `bun:sqlite` database the Streamsy
 * SQLite storage adapter uses, so canonical events, the board projection, and
 * this metadata all persist together in one file and survive restart. Only
 * verifier hashes and capability metadata are stored — never a raw token.
 */

import type { Database } from "bun:sqlite";

import type {
  CapabilityRow,
  CommandRow,
  GameRow,
  GenerationRow,
  GenerationStatus,
  Stores,
} from "./stores.ts";
import type { GameEvent } from "../../src/domain/events.ts";
import type { DecisionError } from "../../src/domain/decide.ts";
import type { CapabilityRole } from "../capabilities.ts";

const SCHEMA = `
create table if not exists risk_capabilities (
  token_id text primary key,
  verifier_hash text not null,
  game_id text not null,
  player_id text not null,
  role text not null,
  created_at integer not null
);
create table if not exists risk_games (
  game_id text primary key,
  source_stream_id text not null,
  projection_stream_id text not null,
  generation text not null,
  created_at integer not null
);
create table if not exists risk_commands (
  game_id text not null,
  command_id text not null,
  payload_hash text not null,
  status text not null,
  source_offset text,
  events_json text,
  error_json text,
  created_at integer not null,
  primary key (game_id, command_id)
);
create table if not exists risk_generations (
  game_id text not null,
  generation text not null,
  stream_id text not null,
  reducer_version text not null,
  status text not null,
  source_through_offset text,
  created_at integer not null,
  primary key (game_id, generation)
);
`;

interface CapabilityDbRow {
  token_id: string;
  verifier_hash: string;
  game_id: string;
  player_id: string;
  role: string;
  created_at: number;
}

interface GameDbRow {
  game_id: string;
  source_stream_id: string;
  projection_stream_id: string;
  generation: string;
  created_at: number;
}

function gameFromDb(r: GameDbRow): GameRow {
  return {
    gameId: r.game_id,
    sourceStreamId: r.source_stream_id,
    projectionStreamId: r.projection_stream_id,
    generation: r.generation,
    createdAt: r.created_at,
  };
}

interface CommandDbRow {
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
  game_id: string;
  generation: string;
  stream_id: string;
  reducer_version: string;
  status: string;
  source_through_offset: string | null;
  created_at: number;
}

function generationFromDb(r: GenerationDbRow): GenerationRow {
  return {
    gameId: r.game_id,
    generation: r.generation,
    streamId: r.stream_id,
    reducerVersion: r.reducer_version,
    status: r.status as GenerationStatus,
    sourceThroughOffset: r.source_through_offset ?? null,
    createdAt: r.created_at,
  };
}

export function initializeRiskSchema(db: Database): void {
  db.run(SCHEMA);
}

export function createSqliteStores(db: Database): Stores {
  initializeRiskSchema(db);

  const insertCapability = db.query(
    `insert or replace into risk_capabilities
       (token_id, verifier_hash, game_id, player_id, role, created_at)
       values (?, ?, ?, ?, ?, ?)`,
  );
  const selectCapability = db.query<CapabilityDbRow, [string]>(
    "select * from risk_capabilities where token_id = ?",
  );
  const insertGame = db.query(
    `insert or replace into risk_games
       (game_id, source_stream_id, projection_stream_id, generation, created_at)
       values (?, ?, ?, ?, ?)`,
  );
  const selectGame = db.query<GameDbRow, [string]>("select * from risk_games where game_id = ?");
  const listGames = db.query<GameDbRow, []>("select * from risk_games order by created_at asc");
  const insertCommand = db.query(
    `insert or replace into risk_commands
       (game_id, command_id, payload_hash, status, source_offset, events_json, error_json, created_at)
       values (?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const selectCommand = db.query<CommandDbRow, [string, string]>(
    "select * from risk_commands where game_id = ? and command_id = ?",
  );
  const insertGeneration = db.query(
    `insert or replace into risk_generations
       (game_id, generation, stream_id, reducer_version, status, source_through_offset, created_at)
       values (?, ?, ?, ?, ?, ?, ?)`,
  );
  const selectGeneration = db.query<GenerationDbRow, [string, string]>(
    "select * from risk_generations where game_id = ? and generation = ?",
  );
  const listGenerations = db.query<GenerationDbRow, [string]>(
    "select * from risk_generations where game_id = ? order by created_at asc",
  );
  const retireActive = db.query(
    "update risk_generations set status = 'retired' where game_id = ? and status = 'active'",
  );
  const markActive = db.query(
    `update risk_generations
       set status = 'active', source_through_offset = ?
       where game_id = ? and generation = ?`,
  );
  const repointGame = db.query("update risk_games set generation = ? where game_id = ?");
  // One transaction repoints the active generation: retire the old, activate the
  // new, and move the game's pointer together — a cutover is all-or-nothing.
  const activateTx = db.transaction(
    (gameId: string, generation: string, sourceThroughOffset: string | null) => {
      const target = selectGeneration.get(gameId, generation);
      if (!target) throw new Error(`unknown generation ${gameId}/${generation}`);
      retireActive.run(gameId);
      markActive.run(sourceThroughOffset, gameId, generation);
      repointGame.run(generation, gameId);
    },
  );

  return {
    capabilities: {
      put(row: CapabilityRow) {
        insertCapability.run(
          row.tokenId,
          row.verifierHash,
          row.gameId,
          row.playerId,
          row.role,
          row.createdAt,
        );
      },
      getByTokenId(tokenId) {
        const r = selectCapability.get(tokenId);
        if (!r) return null;
        return {
          tokenId: r.token_id,
          verifierHash: r.verifier_hash,
          gameId: r.game_id,
          playerId: r.player_id,
          role: r.role as CapabilityRole,
          createdAt: r.created_at,
        };
      },
    },
    games: {
      put(row: GameRow) {
        insertGame.run(
          row.gameId,
          row.sourceStreamId,
          row.projectionStreamId,
          row.generation,
          row.createdAt,
        );
      },
      get(gameId) {
        const r = selectGame.get(gameId);
        return r ? gameFromDb(r) : null;
      },
      list() {
        return listGames.all().map(gameFromDb);
      },
    },
    commands: {
      put(row: CommandRow) {
        insertCommand.run(
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
        const r = selectCommand.get(gameId, commandId);
        if (!r) return null;
        return {
          gameId: r.game_id,
          commandId: r.command_id,
          payloadHash: r.payload_hash,
          status: r.status as CommandRow["status"],
          sourceOffset: r.source_offset ?? undefined,
          events: r.events_json ? (JSON.parse(r.events_json) as GameEvent[]) : undefined,
          error: r.error_json ? (JSON.parse(r.error_json) as DecisionError) : undefined,
          createdAt: r.created_at,
        };
      },
    },
    generations: {
      put(row: GenerationRow) {
        insertGeneration.run(
          row.gameId,
          row.generation,
          row.streamId,
          row.reducerVersion,
          row.status,
          row.sourceThroughOffset ?? null,
          row.createdAt,
        );
      },
      get(gameId, generation) {
        const r = selectGeneration.get(gameId, generation);
        return r ? generationFromDb(r) : null;
      },
      list(gameId) {
        return listGenerations.all(gameId).map(generationFromDb);
      },
      activate(gameId, generation, sourceThroughOffset) {
        activateTx(gameId, generation, sourceThroughOffset);
      },
    },
  };
}
