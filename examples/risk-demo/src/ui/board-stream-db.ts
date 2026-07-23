import { createStateSchema } from "@durable-streams/state";
import {
  createStreamDB,
  type CreateStreamDBOptions,
  type StreamDB,
} from "@durable-streams/state/db";
import { useLiveQuery } from "@tanstack/react-db";
import { useEffect, useState } from "react";
import { z } from "zod";

import type { BoardRows } from "../api.ts";

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

export type RiskBoardDb = StreamDB<typeof riskBoardState>;
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
