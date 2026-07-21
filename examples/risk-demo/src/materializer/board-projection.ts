/**
 * Board projection adapter for the replay-safe {@link ProjectionRuntime}.
 *
 * This binds the pure Batch 1 board reducer ({@link projectEvent}) to the generic
 * projection runtime. Each canonical source event becomes ONE atomic output
 * transaction of Durable State change messages — per-row `game`/`player`/
 * `territory` upserts for downstream consumers, plus a `projectionMeta` row that
 * embeds the canonical `sourceThroughOffset` (the watermark) and a resume
 * snapshot. Because they are appended as a single JSON array in one
 * `ProtocolStream.append`, board changes and their watermark can never commit
 * apart.
 *
 * The canonical event stream stays the source of truth; this projection stream
 * is a separate, rebuildable, causally-watermarked materialization of it.
 */

import type {
  ProjectionAdapter,
  ProjectionCheckpoint,
  ProjectionMeta,
  ProjectionTransition,
} from "@streamsy/experimental/projection";
import type { StreamId, StreamProtocolFactory } from "@streamsy/core";

import type { GameEvent } from "../events.ts";
import { RULESET } from "../map.ts";
import { initialProjection, projectEvent, type ProjectionState } from "../projection.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const BOARD_REDUCER_VERSION = `${RULESET}:board-1`;

type Operation = "insert" | "update";

/** A Durable-State-shaped change message (see `@streamsy/state`). */
interface ChangeMessage {
  type: "game" | "player" | "territory" | "projectionMeta";
  key: string;
  value: unknown;
  headers: { operation: Operation; offset: string };
}

/** The `projectionMeta` row value: the embedded watermark plus a resume snapshot. */
interface ProjectionMetaRow extends ProjectionMeta {
  snapshot: ProjectionState;
}

function change(
  type: ChangeMessage["type"],
  key: string,
  value: unknown,
  operation: Operation,
  offset: string,
): ChangeMessage {
  return { type, key, value, headers: { operation, offset } };
}

function changed(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) !== JSON.stringify(b);
}

export interface BoardProjectionAdapterOptions {
  gameId: string;
  sourceStreamId: StreamId;
  outputStreamId: StreamId;
  processorId?: string;
  generation?: string;
}

/**
 * Build a {@link ProjectionAdapter} that materializes the Risk board projection.
 */
export function createBoardProjectionAdapter(
  options: BoardProjectionAdapterOptions,
): ProjectionAdapter<ProjectionState, GameEvent> {
  const generation = options.generation ?? "v1";
  return {
    processorId: options.processorId ?? `risk-board:${options.gameId}`,
    generation,
    reducerVersion: BOARD_REDUCER_VERSION,
    sourceStreamId: options.sourceStreamId,
    outputStreamId: options.outputStreamId,

    initial: () => initialProjection(),

    decodeSourceMessage: (data) => JSON.parse(decoder.decode(data)) as GameEvent,

    // The real Streamsy source offset becomes the projection's embedded watermark.
    reduce: (state, event, meta) => projectEvent(state, event, meta.sourceThroughOffset),

    encodeTransition: (transition) => encodeBoardTransition(transition, options.gameId),

    decodeCheckpoint: (messages) => decodeBoardCheckpoint(messages),
  };
}

function encodeBoardTransition(
  transition: ProjectionTransition<ProjectionState, GameEvent>,
  gameId: string,
): ChangeMessage[] {
  const { prev, next, meta } = transition;
  const offset = meta.sourceThroughOffset;
  const changes: ChangeMessage[] = [];

  if (changed(prev.game, next.game)) {
    changes.push(change("game", next.game.id ?? gameId, next.game, "update", offset));
  }

  for (const player of next.players) {
    const before = prev.players.find((p) => p.id === player.id);
    if (!before) changes.push(change("player", player.id, player, "insert", offset));
    else if (changed(before, player))
      changes.push(change("player", player.id, player, "update", offset));
  }

  for (const territory of next.territories) {
    const before = prev.territories.find((t) => t.id === territory.id);
    if (!before) changes.push(change("territory", territory.id, territory, "insert", offset));
    else if (changed(before, territory)) {
      changes.push(change("territory", territory.id, territory, "update", offset));
    }
  }

  const metaRow: ProjectionMetaRow = {
    sourceStreamId: meta.sourceStreamId,
    sourceThroughOffset: meta.sourceThroughOffset,
    sourceSeq: meta.sourceSeq,
    generation: meta.generation,
    reducerVersion: meta.reducerVersion,
    snapshot: next,
  };
  changes.push(change("projectionMeta", "board", metaRow, "update", offset));
  return changes;
}

function decodeBoardCheckpoint(
  messages: readonly Uint8Array[],
): ProjectionCheckpoint<ProjectionState> | null {
  let latest: ProjectionMetaRow | null = null;
  for (const data of messages) {
    const value = JSON.parse(decoder.decode(data)) as ChangeMessage;
    if (value.type === "projectionMeta") latest = value.value as ProjectionMetaRow;
  }
  if (!latest) return null;
  return {
    state: latest.snapshot,
    sourceThroughOffset: latest.sourceThroughOffset,
    sourceSeq: latest.sourceSeq,
  };
}

/**
 * Append canonical events to a source stream, one event per message, so each has
 * its own offset and the projection advances exactly one watermark per event.
 */
export async function writeCanonicalEvents(
  protocol: StreamProtocolFactory,
  streamId: StreamId,
  events: readonly GameEvent[],
): Promise<string[]> {
  const created = await protocol.create(streamId, { contentType: "application/json" });
  const stream =
    created.status === "created" || created.status === "exists"
      ? created.stream
      : await requireStream(protocol, streamId);

  const offsets: string[] = [];
  for (const event of events) {
    const appended = await stream.append({
      contentType: "application/json",
      data: encoder.encode(JSON.stringify(event)),
    });
    if (appended.status !== "appended") {
      throw new Error(`cannot append canonical event: ${appended.status}`);
    }
    offsets.push(appended.offset);
  }
  return offsets;
}

async function requireStream(protocol: StreamProtocolFactory, streamId: StreamId) {
  const got = await protocol.get(streamId);
  if (got.status !== "ok") throw new Error(`stream ${streamId} unavailable: ${got.status}`);
  return got.stream;
}
