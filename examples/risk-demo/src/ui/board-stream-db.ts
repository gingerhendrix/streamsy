/* oxlint-disable effecttsgo/async-function -- React and the browser own these Promise-native event and lifecycle callbacks; reusable data orchestration remains behind the existing application facade. */
/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
import { createStateSchema } from "@durable-streams/state";
import {
  createStreamDB,
  type CreateStreamDBOptions,
  type StreamDB,
} from "@durable-streams/state/db";
import { useLiveQuery } from "@tanstack/react-db";
import { useEffect, useState } from "react";
import { z } from "zod";

import type { BoardRows } from "../application/api.ts";
import { compareMoves } from "./attack-trace.ts";

// The client schema mirrors the collections written by the board projection.

const axialSchema = z.object({ q: z.number(), r: z.number() });

const reinforcementSchema = z.object({
  base: z.number(),
  continents: z.array(z.object({ continentId: z.string(), bonus: z.number() })),
  total: z.number(),
  remaining: z.number(),
});

const resolutionSourceSchema = z.enum(["human", "bot", "agent", "timeout"]);

const gameSchema = z.object({
  id: z.string(),
  hostPlayerId: z.string().optional(),
  status: z.enum(["lobby", "playing", "finished"]),
  mapVersion: z.string().optional(),
  generatorVersion: z.string().optional(),
  mapSeed: z.string().optional(),
  round: z.number(),
  activePlayerId: z.string().optional(),
  phase: z.enum(["reinforce", "attack", "fortify"]).optional(),
  winnerId: z.string().optional(),
});

const playerSchema = z.object({
  id: z.string(),
  name: z.string(),
  color: z.string(),
  controller: z.enum(["human", "bot", "external-agent"]),
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

const territorySchema = z.object({
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

const moveSchema = z.object({
  id: z.string(),
  commandId: z.string(),
  kind: z.enum([
    // Every canonical event type reaches the feed, so this enum must list every
    // one of them — a missing member fails the whole batch, not just that row.
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
  ]),
  playerId: z.string().optional(),
  name: z.string().optional(),
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

const projectionStateSchema = z.object({
  game: gameSchema,
  players: z.array(playerSchema),
  hexes: z.array(hexSchema),
  territories: z.array(territorySchema),
  continents: z.array(continentSchema),
  turn: turnSchema.nullable(),
  combat: combatSchema.nullable(),
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

/** Typed Durable State schema for a board generation. */
export const riskBoardState = createStateSchema({
  games: { schema: gameSchema, type: "game", primaryKey: "id" },
  players: { schema: playerSchema, type: "player", primaryKey: "id" },
  hexes: { schema: hexSchema, type: "hex", primaryKey: "id" },
  territories: { schema: territorySchema, type: "territory", primaryKey: "id" },
  continents: { schema: continentSchema, type: "continent", primaryKey: "id" },
  turn: { schema: turnSchema, type: "turn", primaryKey: "id" },
  combat: { schema: combatSchema, type: "combat", primaryKey: "id" },
  moves: { schema: moveSchema, type: "move", primaryKey: "id" },
  projectionMeta: { schema: projectionMetaSchema, type: "projectionMeta", primaryKey: "id" },
});

export type RiskBoardDb = StreamDB<typeof riskBoardState>;
export type SyncStatus = "idle" | "connecting" | "catching-up" | "live" | "error";

function streamUrl(streamId: string): string {
  const path = streamId.split("/").map(encodeURIComponent).join("/");
  return new URL(`/streams/${path}`, window.location.origin).toString();
}

type RiskBoardDbOptions = CreateStreamDBOptions<typeof riskBoardState>;

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
  createDb?: (options: RiskBoardDbOptions) => RiskBoardDb;
}

/** Open one owned, typed materialization of a board projection stream. */
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
  hexes: z.infer<typeof hexSchema>[];
  territories: z.infer<typeof territorySchema>[];
  continents: z.infer<typeof continentSchema>[];
  turn: z.infer<typeof turnSchema>[];
  combat: z.infer<typeof combatSchema>[];
  moves: z.infer<typeof moveSchema>[];
  projectionMeta: z.infer<typeof projectionMetaSchema>[];
}

/**
 * UI-specific shaping of a generation's query results. `turn` and `combat`
 * are zero-or-one collections, so an absent row is `null` rather than an empty
 * array — a cleared combat and "no combat yet" look the same to a renderer, which
 * is exactly right.
 */
export function boardRowsFromQueries(rows: QueryRows): BoardRows | null {
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
    // Newest first, in the feed's own total order: source offset, then event
    // ordinal. Offset alone ties within a delivery boundary. See `compareMoves`.
    moves: rows.moves.toSorted((left, right) => compareMoves(right, left)),
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

/** Own one StreamDB session per active board projection generation. */
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

  const rows = boardRowsFromQueries({
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
