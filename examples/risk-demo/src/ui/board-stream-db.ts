import { createStateSchema } from "@durable-streams/state";
import { createStreamDB, type StreamDB } from "@durable-streams/state/db";
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

function streamUrl(streamId: string): string {
  const path = streamId.split("/").map(encodeURIComponent).join("/");
  return new URL(`/streams/${path}`, window.location.origin).toString();
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
  db: RiskBoardDb | null;
  rows: BoardRows | null;
  status: SyncStatus;
  streamOffset: string | null;
  error: string | null;
}

/** Own one StreamDB connection per active projection generation. */
export function useRiskBoardStream(streamId: string | null): RiskBoardStreamResult {
  const [session, setSession] = useState<{ streamId: string; db: RiskBoardDb } | null>(null);
  const [status, setStatus] = useState<SyncStatus>("idle");
  const [streamOffset, setStreamOffset] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const db = session?.streamId === streamId ? session.db : null;

  useEffect(() => {
    setError(null);
    setStreamOffset(null);
    if (!streamId) {
      setSession(null);
      setStatus("idle");
      return;
    }

    let cancelled = false;
    const created = createStreamDB({
      streamOptions: {
        url: streamUrl(streamId),
        contentType: "application/json",
        warnOnHttp: false,
      },
      live: "long-poll",
      state: riskBoardState,
      onBeforeBatch: () => {
        if (!cancelled) setStatus("catching-up");
      },
      onBatch: (batch) => {
        if (cancelled) return;
        setStreamOffset(batch.offset);
        setStatus(batch.upToDate ? "live" : "catching-up");
      },
    });
    setSession({ streamId, db: created });
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
      created.close();
    };
  }, [streamId]);

  const games = useLiveQuery(
    (query) => (db ? query.from({ games: db.collections.games }) : undefined),
    [db],
  );
  const players = useLiveQuery(
    (query) => (db ? query.from({ players: db.collections.players }) : undefined),
    [db],
  );
  const territories = useLiveQuery(
    (query) => (db ? query.from({ territories: db.collections.territories }) : undefined),
    [db],
  );
  const moves = useLiveQuery(
    (query) => (db ? query.from({ moves: db.collections.moves }) : undefined),
    [db],
  );
  const projectionMeta = useLiveQuery(
    (query) => (db ? query.from({ projectionMeta: db.collections.projectionMeta }) : undefined),
    [db],
  );

  const rows = boardRowsFromQueries({
    games: games.data ?? [],
    players: players.data ?? [],
    territories: territories.data ?? [],
    moves: moves.data ?? [],
    projectionMeta: projectionMeta.data ?? [],
  });

  return { db, rows, status, streamOffset, error };
}
