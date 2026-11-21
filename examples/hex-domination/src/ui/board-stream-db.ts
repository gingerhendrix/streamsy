/* oxlint-disable effecttsgo/async-function -- React and the browser own these Promise-native event and lifecycle callbacks; reusable data orchestration remains behind the existing application facade. */
import { createStateSchema } from "@durable-streams/state";
import {
  createStreamDB,
  type CreateStreamDBOptions,
  type StreamDB,
} from "@durable-streams/state/db";
import { useLiveQuery } from "@tanstack/react-db";
import { Schema } from "effect";
import { useEffect, useState } from "react";

import type { BoardRows } from "../application/api.ts";
import {
  ProjectedCombatSchema,
  ProjectedContinentSchema,
  ProjectedGameSchema,
  ProjectedHexSchema,
  ProjectedMoveSchema,
  ProjectedPlayerSchema,
  ProjectedTerritorySchema,
  ProjectedTurnSchema,
  ProjectionMetaRowSchema,
} from "../board/schemas.ts";
import { compareMoves } from "./attack-trace.ts";

/** Typed Durable State schema for a board generation. */
export const riskBoardState = createStateSchema({
  games: { schema: Schema.toStandardSchemaV1(ProjectedGameSchema), type: "game", primaryKey: "id" },
  players: {
    schema: Schema.toStandardSchemaV1(ProjectedPlayerSchema),
    type: "player",
    primaryKey: "id",
  },
  hexes: { schema: Schema.toStandardSchemaV1(ProjectedHexSchema), type: "hex", primaryKey: "id" },
  territories: {
    schema: Schema.toStandardSchemaV1(ProjectedTerritorySchema),
    type: "territory",
    primaryKey: "id",
  },
  continents: {
    schema: Schema.toStandardSchemaV1(ProjectedContinentSchema),
    type: "continent",
    primaryKey: "id",
  },
  turn: { schema: Schema.toStandardSchemaV1(ProjectedTurnSchema), type: "turn", primaryKey: "id" },
  combat: {
    schema: Schema.toStandardSchemaV1(ProjectedCombatSchema),
    type: "combat",
    primaryKey: "id",
  },
  moves: { schema: Schema.toStandardSchemaV1(ProjectedMoveSchema), type: "move", primaryKey: "id" },
  projectionMeta: {
    schema: Schema.toStandardSchemaV1(ProjectionMetaRowSchema),
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
  games: (typeof ProjectedGameSchema.Type)[];
  players: (typeof ProjectedPlayerSchema.Type)[];
  hexes: (typeof ProjectedHexSchema.Type)[];
  territories: (typeof ProjectedTerritorySchema.Type)[];
  continents: (typeof ProjectedContinentSchema.Type)[];
  turn: (typeof ProjectedTurnSchema.Type)[];
  combat: (typeof ProjectedCombatSchema.Type)[];
  moves: (typeof ProjectedMoveSchema.Type)[];
  projectionMeta: (typeof ProjectionMetaRowSchema.Type)[];
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
      return undefined;
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
        if (cancelled) return undefined;
        setStreamOffset(created.offset);
        setStatus("live");
        return undefined;
      },
      (reason) => {
        if (cancelled) return undefined;
        setError(reason instanceof Error ? reason.message : String(reason));
        setStatus("error");
        return undefined;
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
