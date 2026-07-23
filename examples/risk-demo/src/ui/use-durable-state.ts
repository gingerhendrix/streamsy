import { officialProtocolClient, protocolPathUrl, type JsonValue } from "@streamsy/client";
import { useEffect, useState } from "react";

import type { BoardProjectionMeta, BoardRows } from "../api.ts";
import type {
  ProjectedGame,
  ProjectedMove,
  ProjectedPlayer,
  ProjectedTerritory,
} from "../projection.ts";

type CollectionName = "game" | "player" | "territory" | "move" | "projectionMeta";

export interface DurableStateClientSchema {
  types: ReadonlySet<CollectionName>;
}

export const boardStateSchema: DurableStateClientSchema = {
  types: new Set(["game", "player", "territory", "move", "projectionMeta"]),
};

export interface DurableStateChange {
  type?: CollectionName;
  key?: string;
  value?: unknown;
  headers: {
    operation?: "insert" | "update" | "delete";
    control?: "snapshot-start" | "snapshot-end" | "reset";
    offset?: string;
  };
}

export interface BoardCollectionState {
  game: Map<string, ProjectedGame>;
  players: Map<string, ProjectedPlayer>;
  territories: Map<string, ProjectedTerritory>;
  moves: Map<string, ProjectedMove>;
  meta: BoardProjectionMeta | null;
}

export function emptyBoardCollections(): BoardCollectionState {
  return {
    game: new Map(),
    players: new Map(),
    territories: new Map(),
    moves: new Map(),
    meta: null,
  };
}

/** Apply wire-level Durable State changes without coupling React to storage details. */
export function applyBoardChanges(
  previous: BoardCollectionState,
  changes: readonly DurableStateChange[],
): BoardCollectionState {
  let next = previous;
  const mutable = () => {
    if (next !== previous) return;
    next = {
      game: new Map(previous.game),
      players: new Map(previous.players),
      territories: new Map(previous.territories),
      moves: new Map(previous.moves),
      meta: previous.meta,
    };
  };

  for (const change of changes) {
    if (change.headers.control === "reset" || change.headers.control === "snapshot-start") {
      next = emptyBoardCollections();
      continue;
    }
    if (!change.type || !change.key || !change.headers.operation) continue;
    mutable();
    if (change.type === "projectionMeta") {
      next.meta =
        change.headers.operation === "delete" ? null : (change.value as BoardProjectionMeta);
      continue;
    }
    const collection =
      change.type === "game"
        ? next.game
        : change.type === "player"
          ? next.players
          : change.type === "territory"
            ? next.territories
            : next.moves;
    if (change.headers.operation === "delete") collection.delete(change.key);
    else collection.set(change.key, change.value as never);
  }
  return next;
}

export function boardRows(state: BoardCollectionState): BoardRows | null {
  const game = state.game.values().next().value;
  if (!game) return null;
  return {
    game,
    players: [...state.players.values()],
    territories: [...state.territories.values()],
    moves: [...state.moves.values()].toSorted((a, b) =>
      b.sourceOffset.localeCompare(a.sourceOffset),
    ),
    meta: state.meta,
  };
}

export type SyncStatus = "idle" | "connecting" | "catching-up" | "live" | "reconnecting" | "error";

export interface DurableStateResult {
  rows: BoardRows | null;
  status: SyncStatus;
  streamOffset: string | null;
  error: string | null;
}

const delay = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = window.setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });

/** Demo-local live Durable State hook backed by Streamsy's official long-poll client. */
export function useDurableState(
  streamId: string | null,
  schema: DurableStateClientSchema,
): DurableStateResult {
  const [collections, setCollections] = useState<BoardCollectionState>(emptyBoardCollections);
  const [status, setStatus] = useState<SyncStatus>("idle");
  const [streamOffset, setStreamOffset] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setCollections(emptyBoardCollections());
    setStreamOffset(null);
    setError(null);
    if (!streamId) {
      setStatus("idle");
      return;
    }

    const controller = new AbortController();
    const client = officialProtocolClient({
      urlFor: (id) => protocolPathUrl(`${window.location.origin}/streams`, id),
      signal: controller.signal,
      backoffOptions: { initialDelay: 250, maxDelay: 4_000, multiplier: 1.8, maxRetries: 8 },
      warnOnHttp: false,
    });

    void (async () => {
      let first = true;
      while (!controller.signal.aborted) {
        setStatus(first ? "connecting" : "reconnecting");
        const read = await client.stream(streamId).read<JsonValue>({
          live: "long-poll",
          signal: controller.signal,
        });
        if (read.status !== "ok") {
          if (controller.signal.aborted) break;
          setError(read.status === "error" ? read.message : `Board stream ${read.status}`);
          setStatus("error");
          await delay(800, controller.signal);
          first = false;
          continue;
        }

        setStatus("catching-up");
        for await (const batch of read.session) {
          if (batch.kind !== "json") continue;
          const changes = (batch.items as unknown as DurableStateChange[]).filter(
            (change) => !change.type || schema.types.has(change.type),
          );
          setCollections((current) => applyBoardChanges(current, changes));
          setStreamOffset(batch.offset);
          setStatus(batch.upToDate ? "live" : "catching-up");
          setError(null);
        }
        const end = await read.session.done;
        if (controller.signal.aborted || end.status === "cancelled") break;
        if (end.status === "error") setError(end.message);
        first = false;
        await delay(500, controller.signal);
      }
      await client.close();
    })();

    return () => {
      controller.abort("board subscription changed");
      void client.close();
    };
  }, [schema, streamId]);

  return { rows: boardRows(collections), status, streamOffset, error };
}
