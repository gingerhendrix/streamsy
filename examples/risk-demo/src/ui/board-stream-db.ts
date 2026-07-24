import { createStateSchema } from "@durable-streams/state";
import {
  createStreamDB,
  type CreateStreamDBOptions,
  type StreamDB,
} from "@durable-streams/state/db";
import { useLiveQuery } from "@tanstack/react-db";
import { useEffect, useState } from "react";
import { z } from "zod";

import type { BoardRows, BoardRowsV2 } from "../application/api.ts";

const gameSchema = z.object({
  id: z.string(),
  status: z.enum(["lobby", "playing", "finished"]),
  phase: z.enum(["reinforce", "attack", "fortify"]).optional(),
  activePlayerId: z.string().optional(),
  round: z.number(),
  winnerId: z.string().optional(),
});

const playerSchema = z.object({
  id: z.string(),
  name: z.string(),
  color: z.string(),
  remainingArmies: z.number(),
  eliminated: z.boolean(),
});

const territorySchema = z.object({
  id: z.string(),
  ownerId: z.string().optional(),
  armies: z.number(),
});

const moveSchema = z.object({
  id: z.string(),
  commandId: z.string(),
  kind: z.enum([
    "GameCreated",
    "PlayerJoined",
    "GameStarted",
    "ArmiesReinforced",
    "AttackResolved",
    "ArmiesFortified",
    "TurnEnded",
    "PlayerEliminated",
    "GameWon",
  ]),
  playerId: z.string().optional(),
  sourceOffset: z.string(),
  territoryId: z.string().optional(),
  from: z.string().optional(),
  to: z.string().optional(),
  armies: z.number().optional(),
  attackerRolls: z.array(z.number()).optional(),
  defenderRolls: z.array(z.number()).optional(),
  attackerLosses: z.number().optional(),
  defenderLosses: z.number().optional(),
  territoryCaptured: z.boolean().optional(),
  nextPlayerId: z.string().optional(),
});

const projectionStateSchema = z.object({
  game: gameSchema,
  players: z.array(playerSchema),
  territories: z.array(territorySchema),
  moves: z.array(moveSchema),
  sourceThroughOffset: z.string().nullable(),
});

const projectionMetaSchema = z.object({
  id: z.string(),
  sourceStreamId: z.string(),
  sourceThroughOffset: z.string(),
  sourceSeq: z.number(),
  generation: z.string(),
  reducerVersion: z.string(),
  snapshot: projectionStateSchema,
});

/** Typed Durable State schema consumed by the browser's StreamDB. */
export const riskBoardState = createStateSchema({
  games: { schema: gameSchema, type: "game", primaryKey: "id" },
  players: { schema: playerSchema, type: "player", primaryKey: "id" },
  territories: { schema: territorySchema, type: "territory", primaryKey: "id" },
  moves: { schema: moveSchema, type: "move", primaryKey: "id" },
  projectionMeta: {
    schema: projectionMetaSchema,
    type: "projectionMeta",
    primaryKey: "id",
  },
});

// ---------------------------------------------------------------------------
// risk-demo-v2 collections
//
// Client-side schema only: the SVG hex renderer, turn rail, and dice experience
// land in the next batch. What is here is the typed surface they consume — the
// same eight collections the v2 projection writes, so a Batch 4 component can
// live-query `combat` or `turn` without decoding a stream itself.
// ---------------------------------------------------------------------------

const axialSchema = z.object({ q: z.number(), r: z.number() });

const reinforcementSchema = z.object({
  base: z.number(),
  continents: z.array(z.object({ continentId: z.string(), bonus: z.number() })),
  total: z.number(),
  remaining: z.number(),
});

const resolutionSourceSchema = z.enum(["human", "agent-auto", "timeout"]);

const gameV2Schema = z.object({
  id: z.string(),
  hostPlayerId: z.string().optional(),
  status: z.enum(["lobby", "playing", "finished"]),
  ruleset: z.string().optional(),
  mapVersion: z.string().optional(),
  generatorVersion: z.string().optional(),
  mapSeed: z.string().optional(),
  round: z.number(),
  activePlayerId: z.string().optional(),
  phase: z.enum(["reinforce", "attack", "fortify"]).optional(),
  winnerId: z.string().optional(),
});

const playerV2Schema = z.object({
  id: z.string(),
  name: z.string(),
  color: z.string(),
  controller: z.enum(["human", "agent"]),
  eliminated: z.boolean(),
  territoryCount: z.number(),
  armyCount: z.number(),
});

const hexSchema = z.object({
  id: z.string(),
  q: z.number(),
  r: z.number(),
  territoryId: z.string(),
  terrain: z.enum(["plains", "forest", "hills", "desert", "mountains"]),
});

const territoryV2Schema = z.object({
  id: z.string(),
  name: z.string(),
  continentId: z.string(),
  ownerId: z.string().optional(),
  armies: z.number(),
  hexIds: z.array(z.string()),
  adjacentTerritoryIds: z.array(z.string()),
  labelAnchor: axialSchema,
});

const continentSchema = z.object({
  id: z.string(),
  name: z.string(),
  territoryIds: z.array(z.string()),
  reinforcementBonus: z.number(),
  controllerId: z.string().optional(),
  palette: z.object({ hue: z.number(), pattern: z.string() }),
});

const turnSchema = z.object({
  id: z.string(),
  turnId: z.string(),
  round: z.number(),
  playerId: z.string(),
  phase: z.enum(["reinforce", "attack", "fortify"]).optional(),
  reinforcement: reinforcementSchema,
  reinforcementsPlaced: z.number(),
  attacksDeclared: z.number(),
  throwsResolved: z.number(),
  captures: z.number(),
  eliminations: z.number(),
  latestDice: z
    .object({
      attackId: z.string(),
      from: z.string(),
      to: z.string(),
      attackerRolls: z.array(z.number()),
      defenderRolls: z.array(z.number()),
      attackerLosses: z.number(),
      defenderLosses: z.number(),
      territoryCaptured: z.boolean(),
      resolutionSource: resolutionSourceSchema,
    })
    .optional(),
});

const combatSchema = z.object({
  id: z.string(),
  attackId: z.string(),
  turnId: z.string(),
  status: z.enum(["awaiting-defense", "awaiting-occupation"]),
  attackerId: z.string(),
  defenderId: z.string(),
  from: z.string(),
  to: z.string(),
  attackerDice: z.number(),
  attackerRolls: z.array(z.number()),
  defenderDice: z.number(),
  declaredAt: z.number(),
  defenseDeadlineAt: z.number(),
  defenderRolls: z.array(z.number()).optional(),
  attackerLosses: z.number().optional(),
  defenderLosses: z.number().optional(),
  territoryCaptured: z.boolean().optional(),
  resolutionSource: resolutionSourceSchema.optional(),
  minArmies: z.number().optional(),
  maxArmies: z.number().optional(),
});

const moveV2Schema = z.object({
  id: z.string(),
  commandId: z.string(),
  kind: z.enum([
    "GameCreated",
    "PlayerJoined",
    "GameStarted",
    "ArmiesReinforced",
    "AttackDeclared",
    "AttackResolved",
    "TerritoryOccupied",
    "ArmiesFortified",
    "PlayerEliminated",
    "TurnEnded",
    "GameWon",
  ]),
  playerId: z.string().optional(),
  sourceOffset: z.string(),
  turnId: z.string().optional(),
  attackId: z.string().optional(),
  territoryId: z.string().optional(),
  from: z.string().optional(),
  to: z.string().optional(),
  armies: z.number().optional(),
  attackerRolls: z.array(z.number()).optional(),
  defenderRolls: z.array(z.number()).optional(),
  attackerLosses: z.number().optional(),
  defenderLosses: z.number().optional(),
  territoryCaptured: z.boolean().optional(),
  resolutionSource: resolutionSourceSchema.optional(),
  nextPlayerId: z.string().optional(),
});

const projectionStateV2Schema = z.object({
  game: gameV2Schema,
  players: z.array(playerV2Schema),
  hexes: z.array(hexSchema),
  territories: z.array(territoryV2Schema),
  continents: z.array(continentSchema),
  turn: turnSchema.nullable(),
  combat: combatSchema.nullable(),
  moves: z.array(moveV2Schema),
  sourceThroughOffset: z.string().nullable(),
});

const projectionMetaV2Schema = z.object({
  id: z.string(),
  sourceStreamId: z.string(),
  sourceThroughOffset: z.string(),
  sourceSeq: z.number(),
  generation: z.string(),
  reducerVersion: z.string(),
  snapshot: projectionStateV2Schema,
});

/** Typed Durable State schema for a `risk-demo-v2` board generation. */
export const riskBoardStateV2 = createStateSchema({
  games: { schema: gameV2Schema, type: "game", primaryKey: "id" },
  players: { schema: playerV2Schema, type: "player", primaryKey: "id" },
  hexes: { schema: hexSchema, type: "hex", primaryKey: "id" },
  territories: { schema: territoryV2Schema, type: "territory", primaryKey: "id" },
  continents: { schema: continentSchema, type: "continent", primaryKey: "id" },
  turn: { schema: turnSchema, type: "turn", primaryKey: "id" },
  combat: { schema: combatSchema, type: "combat", primaryKey: "id" },
  moves: { schema: moveV2Schema, type: "move", primaryKey: "id" },
  projectionMeta: { schema: projectionMetaV2Schema, type: "projectionMeta", primaryKey: "id" },
});

export type RiskBoardDb = StreamDB<typeof riskBoardState>;
export type RiskBoardV2Db = StreamDB<typeof riskBoardStateV2>;
export type SyncStatus = "idle" | "connecting" | "catching-up" | "live" | "error";

type RiskBoardDbOptions = CreateStreamDBOptions<typeof riskBoardState>;
type RiskBoardDbFactory = (options: RiskBoardDbOptions) => RiskBoardDb;

export interface RiskBoardSession {
  readonly db: RiskBoardDb;
  readonly collections: RiskBoardDb["collections"];
  readonly offset: string;
  preload(): Promise<void>;
  awaitTxId(txid: string, timeoutMs?: number): Promise<void>;
  close(): Promise<void>;
}

export interface CreateRiskBoardSessionOptions {
  streamId: string;
  onBeforeBatch?: NonNullable<RiskBoardDbOptions["onBeforeBatch"]>;
  onBatch?: NonNullable<RiskBoardDbOptions["onBatch"]>;
  /** Test seam; production always uses the official Stream DB factory. */
  createDb?: RiskBoardDbFactory;
}

function streamUrl(streamId: string): string {
  const path = streamId.split("/").map(encodeURIComponent).join("/");
  return new URL(`/streams/${path}`, window.location.origin).toString();
}

/** Open one owned, typed materialization of a Risk board projection stream. */
export function createRiskBoardSession(options: CreateRiskBoardSessionOptions): RiskBoardSession {
  const db = (options.createDb ?? createStreamDB)({
    streamOptions: {
      url: streamUrl(options.streamId),
      contentType: "application/json",
      warnOnHttp: false,
    },
    live: "long-poll",
    state: riskBoardState,
    onBeforeBatch: options.onBeforeBatch,
    onBatch: options.onBatch,
  });
  let closing: Promise<void> | undefined;

  return {
    db,
    collections: db.collections,
    get offset() {
      return db.offset;
    },
    preload: () => db.preload(),
    awaitTxId: (txid, timeoutMs) => db.utils.awaitTxId(txid, timeoutMs),
    close: () => {
      closing ??= (async () => {
        db.close();
      })();
      return closing;
    },
  };
}

interface QueryRows {
  games: z.infer<typeof gameSchema>[];
  players: z.infer<typeof playerSchema>[];
  territories: z.infer<typeof territorySchema>[];
  moves: z.infer<typeof moveSchema>[];
  projectionMeta: z.infer<typeof projectionMetaSchema>[];
}

/** UI-specific shaping on top of TanStack query results; no stream decoding lives here. */
export function boardRowsFromQueries(rows: QueryRows): BoardRows | null {
  const game = rows.games[0];
  if (!game) return null;
  return {
    game,
    players: rows.players,
    territories: rows.territories,
    moves: rows.moves.toSorted((left, right) =>
      right.sourceOffset.localeCompare(left.sourceOffset),
    ),
    meta: rows.projectionMeta[0] ?? null,
  };
}

type RiskBoardV2DbOptions = CreateStreamDBOptions<typeof riskBoardStateV2>;

export interface RiskBoardV2Session {
  readonly db: RiskBoardV2Db;
  readonly collections: RiskBoardV2Db["collections"];
  readonly offset: string;
  preload(): Promise<void>;
  awaitTxId(txid: string, timeoutMs?: number): Promise<void>;
  close(): Promise<void>;
}

export interface CreateRiskBoardV2SessionOptions {
  streamId: string;
  onBeforeBatch?: NonNullable<RiskBoardV2DbOptions["onBeforeBatch"]>;
  onBatch?: NonNullable<RiskBoardV2DbOptions["onBatch"]>;
  /** Test seam; production always uses the official Stream DB factory. */
  createDb?: (options: RiskBoardV2DbOptions) => RiskBoardV2Db;
}

/** Open one owned, typed materialization of a v2 board projection stream. */
export function createRiskBoardV2Session(
  options: CreateRiskBoardV2SessionOptions,
): RiskBoardV2Session {
  const db = (options.createDb ?? createStreamDB)({
    streamOptions: {
      url: streamUrl(options.streamId),
      contentType: "application/json",
      warnOnHttp: false,
    },
    live: "long-poll",
    state: riskBoardStateV2,
    onBeforeBatch: options.onBeforeBatch,
    onBatch: options.onBatch,
  });
  let closing: Promise<void> | undefined;

  return {
    db,
    collections: db.collections,
    get offset() {
      return db.offset;
    },
    preload: () => db.preload(),
    awaitTxId: (txid, timeoutMs) => db.utils.awaitTxId(txid, timeoutMs),
    close: () => {
      closing ??= (async () => {
        db.close();
      })();
      return closing;
    },
  };
}

interface QueryRowsV2 {
  games: z.infer<typeof gameV2Schema>[];
  players: z.infer<typeof playerV2Schema>[];
  hexes: z.infer<typeof hexSchema>[];
  territories: z.infer<typeof territoryV2Schema>[];
  continents: z.infer<typeof continentSchema>[];
  turn: z.infer<typeof turnSchema>[];
  combat: z.infer<typeof combatSchema>[];
  moves: z.infer<typeof moveV2Schema>[];
  projectionMeta: z.infer<typeof projectionMetaV2Schema>[];
}

/**
 * UI-specific shaping of a v2 generation's query results. `turn` and `combat`
 * are zero-or-one collections, so an absent row is `null` rather than an empty
 * array — a cleared combat and "no combat yet" look the same to a renderer, which
 * is exactly right.
 */
export function boardRowsV2FromQueries(rows: QueryRowsV2): BoardRowsV2 | null {
  const game = rows.games[0];
  if (!game) return null;
  return {
    game,
    players: rows.players,
    hexes: rows.hexes,
    territories: rows.territories,
    continents: rows.continents,
    turn: rows.turn[0] ?? null,
    combat: rows.combat[0] ?? null,
    moves: rows.moves.toSorted((left, right) =>
      right.sourceOffset.localeCompare(left.sourceOffset),
    ),
    meta: rows.projectionMeta[0] ?? null,
  };
}

export interface RiskBoardStreamResult {
  session: RiskBoardSession | null;
  rows: BoardRows | null;
  status: SyncStatus;
  streamOffset: string | null;
  error: string | null;
}

/** Own one StreamDB session per active projection generation. */
export function useRiskBoardStream(streamId: string | null): RiskBoardStreamResult {
  const [active, setActive] = useState<{
    streamId: string;
    session: RiskBoardSession;
  } | null>(null);
  const [status, setStatus] = useState<SyncStatus>("idle");
  const [streamOffset, setStreamOffset] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const session = active?.streamId === streamId ? active.session : null;

  useEffect(() => {
    setError(null);
    setStreamOffset(null);
    if (!streamId) {
      setActive(null);
      setStatus("idle");
      return;
    }

    let cancelled = false;
    const created = createRiskBoardSession({
      streamId,
      onBeforeBatch: () => {
        if (!cancelled) setStatus("catching-up");
      },
      onBatch: (batch) => {
        if (cancelled) return;
        setStreamOffset(batch.offset);
        setStatus(batch.upToDate ? "live" : "catching-up");
      },
    });
    setActive({ streamId, session: created });
    setStatus("connecting");

    void created.preload().then(
      () => {
        if (cancelled) return;
        setStreamOffset(created.offset);
        setStatus("live");
      },
      (reason) => {
        if (cancelled) return;
        setError(reason instanceof Error ? reason.message : String(reason));
        setStatus("error");
      },
    );

    return () => {
      cancelled = true;
      void created.close();
    };
  }, [streamId]);

  const games = useLiveQuery(
    (query) => (session ? query.from({ games: session.collections.games }) : undefined),
    [session],
  );
  const players = useLiveQuery(
    (query) => (session ? query.from({ players: session.collections.players }) : undefined),
    [session],
  );
  const territories = useLiveQuery(
    (query) => (session ? query.from({ territories: session.collections.territories }) : undefined),
    [session],
  );
  const moves = useLiveQuery(
    (query) => (session ? query.from({ moves: session.collections.moves }) : undefined),
    [session],
  );
  const projectionMeta = useLiveQuery(
    (query) =>
      session ? query.from({ projectionMeta: session.collections.projectionMeta }) : undefined,
    [session],
  );

  const rows = boardRowsFromQueries({
    games: games.data ?? [],
    players: players.data ?? [],
    territories: territories.data ?? [],
    moves: moves.data ?? [],
    projectionMeta: projectionMeta.data ?? [],
  });

  return { session, rows, status, streamOffset, error };
}

export interface RiskBoardV2StreamResult {
  session: RiskBoardV2Session | null;
  rows: BoardRowsV2 | null;
  status: SyncStatus;
  streamOffset: string | null;
  error: string | null;
}

/**
 * Own one StreamDB session per active v2 projection generation.
 *
 * Structurally identical to {@link useRiskBoardStream} — a separate hook rather
 * than a parameterised one because the two rulesets have different collections,
 * and a game's renderer is chosen from its canonical `ruleset` (design spec §11).
 */
export function useRiskBoardV2Stream(streamId: string | null): RiskBoardV2StreamResult {
  const [active, setActive] = useState<{
    streamId: string;
    session: RiskBoardV2Session;
  } | null>(null);
  const [status, setStatus] = useState<SyncStatus>("idle");
  const [streamOffset, setStreamOffset] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const session = active?.streamId === streamId ? active.session : null;

  useEffect(() => {
    setError(null);
    setStreamOffset(null);
    if (!streamId) {
      setActive(null);
      setStatus("idle");
      return;
    }

    let cancelled = false;
    const created = createRiskBoardV2Session({
      streamId,
      onBeforeBatch: () => {
        if (!cancelled) setStatus("catching-up");
      },
      onBatch: (batch) => {
        if (cancelled) return;
        setStreamOffset(batch.offset);
        setStatus(batch.upToDate ? "live" : "catching-up");
      },
    });
    setActive({ streamId, session: created });
    setStatus("connecting");

    void created.preload().then(
      () => {
        if (cancelled) return;
        setStreamOffset(created.offset);
        setStatus("live");
      },
      (reason) => {
        if (cancelled) return;
        setError(reason instanceof Error ? reason.message : String(reason));
        setStatus("error");
      },
    );

    return () => {
      cancelled = true;
      void created.close();
    };
  }, [streamId]);

  const games = useLiveQuery(
    (query) => (session ? query.from({ games: session.collections.games }) : undefined),
    [session],
  );
  const players = useLiveQuery(
    (query) => (session ? query.from({ players: session.collections.players }) : undefined),
    [session],
  );
  const hexes = useLiveQuery(
    (query) => (session ? query.from({ hexes: session.collections.hexes }) : undefined),
    [session],
  );
  const territories = useLiveQuery(
    (query) => (session ? query.from({ territories: session.collections.territories }) : undefined),
    [session],
  );
  const continents = useLiveQuery(
    (query) => (session ? query.from({ continents: session.collections.continents }) : undefined),
    [session],
  );
  const turn = useLiveQuery(
    (query) => (session ? query.from({ turn: session.collections.turn }) : undefined),
    [session],
  );
  const combat = useLiveQuery(
    (query) => (session ? query.from({ combat: session.collections.combat }) : undefined),
    [session],
  );
  const moves = useLiveQuery(
    (query) => (session ? query.from({ moves: session.collections.moves }) : undefined),
    [session],
  );
  const projectionMeta = useLiveQuery(
    (query) =>
      session ? query.from({ projectionMeta: session.collections.projectionMeta }) : undefined,
    [session],
  );

  const rows = boardRowsV2FromQueries({
    games: games.data ?? [],
    players: players.data ?? [],
    hexes: hexes.data ?? [],
    territories: territories.data ?? [],
    continents: continents.data ?? [],
    turn: turn.data ?? [],
    combat: combat.data ?? [],
    moves: moves.data ?? [],
    projectionMeta: projectionMeta.data ?? [],
  });

  return { session, rows, status, streamOffset, error };
}
